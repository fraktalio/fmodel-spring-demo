package com.fraktalio.example.fmodelspringdemo.adapter.eventstream

import com.fraktalio.example.fmodelspringdemo.SpringCoroutineScope
import com.fraktalio.example.fmodelspringdemo.adapter.deciderId
import com.fraktalio.example.fmodelspringdemo.application.MaterializedViewState
import com.fraktalio.example.fmodelspringdemo.domain.Command
import com.fraktalio.example.fmodelspringdemo.domain.Event
import com.fraktalio.fmodel.application.MaterializedView
import com.fraktalio.fmodel.application.SagaManager
import com.fraktalio.fmodel.application.handle
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.channels.Channel.Factory.BUFFERED
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import java.time.LocalDateTime
import java.time.Month
import kotlin.coroutines.cancellation.CancellationException


private val logger = KotlinLogging.logger {}

/**
 * Event stream subscriber/processor
 *
 * It extends [SpringCoroutineScope] to map the Spring bean lifecycle to the kotlin coroutine scope in a robust way.
 */
class EventStreamProcessor(
    private val transactionalOperator: TransactionalOperator,
    private val eventStreamingRepository: EventStreamRepository,
    private val lockRepository: LockRepository,
    private val viewRepository: ViewRepository,
    private val materializedView: MaterializedView<MaterializedViewState, Event?>? = null,
    private val sagaManager: SagaManager<Event?, Command>? = null
) : SpringCoroutineScope by SpringCoroutineScope(eventStreamDispatcher) {
    /**
     * Creates a stream of events for the given view.
     *
     * @param view the view name
     */
    private fun streamEvents(
        @Suppress("SameParameterValue") view: String,
    ): Flow<Pair<Event, Long>> =
        flow {
            when (val viewEntity = viewRepository.findById(view)) {
                null -> logger.warn { "view $view not found. can not open the event stream!" }
                else -> eventStreamingRepository.streamEvents(view, viewEntity.poolingDelayMilliseconds)
                    .collect { emit(it) }

            }
        }

    /**
     * Register a materialized view and start pooling events by using [streamEvents] function
     * @param view the view name
     * @param materializedView the materialized view to register - event handler
     * @param buffer the buffer size - the emitter (DB pooling) is suspended when the buffer overflows, to let slow collector (materialized view handler) catch up
     * @param retries the number of retries in case of an exception while streaming (not handling) the events: db exceptions, network exceptions, etc.
     * @param nackInMilliseconds scheduling retries in case of an exception while handling the events / the number of milliseconds to wait before retrying to handle the event
     */
    private fun registerEventHandlerAndStartPooling(
        view: String,
        materializedView: MaterializedView<MaterializedViewState, Event?>,
        buffer: Int = BUFFERED,
        retries: Long = 5,
        nackInMilliseconds: Long = 10000
    ) {
        launch {
            streamEvents(view)
                .onStart {
                    logger.info { "starting event streaming for the view 'view' ..." }
                }
                .retry(retries) { cause ->
                    cause !is CancellationException
                }
                .onCompletion {
                    when (it) {
                        null -> logger.info { "event stream closed successfully" }
                        else -> logger.warn(it) { "event stream closed exceptionally: $it" }
                    }
                }
                .buffer(buffer)
                .collect {
                    // The event handler and lock/token management are executed in one transaction
                    try {
                        transactionalOperator.executeAndAwait { reactiveTransaction ->
                            try {
                                materializedView.handle(it.first)
                                lockRepository.executeAction(view, Ack(it.second, it.first.deciderId()))
                                logger.debug { "handled event successfully: $it" }
                            } catch (e: Exception) {
                                logger.error { "handled event exceptionally: $e" }
                                reactiveTransaction.setRollbackOnly()
                                throw e
                            }
                        }
                    } catch (e: Exception) {
                        lockRepository.executeAction(view, ScheduleNack(nackInMilliseconds, it.first.deciderId()))
                        logger.debug { "handled event unsuccessfully. setting ScheduleNack/RETRY in 10 seconds, for: $it" }
                    }
                }
        }
    }

    /**
     * Register a saga manager and start pooling events by using [streamEvents] function
     * @param view the view name
     * @param sagaManager the saga manager to register - event handler
     * @param buffer the buffer size - the emitter (DB pooling) is suspended when the buffer overflows, to let slow collector (saga manager handler) catch up
     * @param retries the number of retries in case of an exception while streaming (not handling) the events: db exceptions, network exceptions, etc.
     * @param nackInMilliseconds scheduling retries in case of an exception while handling the events / the number of milliseconds to wait before retrying to handle the event
     */
    private fun registerSagaManagerAndStartPooling(
        view: String,
        sagaManager: SagaManager<Event?, Command>,
        buffer: Int = BUFFERED,
        retries: Long = 5,
        nackInMilliseconds: Long = 10000
    ) {
        launch {
            streamEvents(view)
                .onStart {
                    logger.info { "saga: starting event streaming for the saga/view 'saga' ..." }
                }
                .retry(retries) { cause ->
                    cause !is CancellationException
                }
                .onCompletion {
                    when (it) {
                        null -> logger.info { "saga: event stream closed successfully" }
                        else -> logger.warn(it) { "saga: event stream closed exceptionally: $it" }
                    }
                }
                .buffer(buffer)
                .collect {
                    // The saga manager and lock/token management are executed in one transaction
                    try {
                        transactionalOperator.executeAndAwait { reactiveTransaction ->
                            try {
                                sagaManager.handle(it.first).collect()
                                lockRepository.executeAction(view, Ack(it.second, it.first.deciderId()))
                                logger.debug { "saga: handled event successfully: $it" }
                            } catch (e: Exception) {
                                logger.error { "saga: handled event exceptionally: $e" }
                                reactiveTransaction.setRollbackOnly()
                                throw e
                            }
                        }
                    } catch (e: Exception) {
                        lockRepository.executeAction(view, ScheduleNack(nackInMilliseconds, it.first.deciderId()))
                        logger.debug { "saga: handled event unsuccessfully. setting ScheduleNack/RETRY in 10 seconds, for: $it" }
                    }
                }
        }
    }

    /**
     * Register event handler and saga manager on Spring ContextRefreshedEvent
     */
    @EventListener
    fun onApplicationEvent(event: ContextRefreshedEvent?) {
        logger.info { "spring context refreshed: $event" }
        sagaManager?.let { registerSagaManagerAndStartPooling("saga", it) }
        materializedView?.let { registerEventHandlerAndStartPooling("view", it) }
    }
}

/**
 * Kotlin coroutine dispatcher for event stream processing, with limited parallelism.
 */
@OptIn(ExperimentalCoroutinesApi::class)
private val eventStreamDispatcher = Dispatchers.IO.limitedParallelism(10)


@Serializable
sealed class Action

@Serializable
data class Ack(val offset: Long, val deciderId: String) : Action()

@Serializable
data class Nack(val deciderId: String) : Action()

@Serializable
data class ScheduleNack(val milliseconds: Long, val deciderId: String) : Action()

@Serializable
data class Lock(
    val view: String,
    val deciderId: String,
    val offset: Long,
    val lastOffset: Long,
    val lockedUntil: String?,
    val offsetFinal: Boolean,
    val createdAt: String?,
    val updatedAt: String?,
)

@Serializable
data class View(val view: String, val poolingDelayMilliseconds: Long)

/**
 * View repository interface - register the materialized view, find all registered views, find view by id
 *
 * You need to register a view before you can start polling/streaming events for it.
 */
interface ViewRepository {
    /**
     * Register a materialized view / You need to register a view before you can start polling/streaming events for it.
     * @param view the view name
     * @param poolingDelayMilliseconds the pooling delay in milliseconds
     * @param startAt the start date time
     */
    suspend fun registerView(
        view: String,
        poolingDelayMilliseconds: Long = 500L,
        startAt: LocalDateTime = LocalDateTime.of(1, Month.JANUARY, 1, 1, 1)
    ): View

    /**
     * Find all registered materialized views
     */
    fun findAll(): Flow<View>

    /**
     * Find materialized view by id
     */
    suspend fun findById(view: String): View?
}

/**
 * Lock repository interface - find all locks, execute action (ACK, NACK, SCHEDULE_NACK)
 *
 * Enables multiple, concurrent, uncoordinated consumers to safely read/stream events per view
 * The lock is automatically acquired for a certain amount of time (default 5 seconds) on every read of an event for the decider stream/partition
 * and then released with ACK or NACK
 */
interface LockRepository {
    /**
     * Find all locks
     */
    fun findAll(): Flow<Lock>

    /**
     * Execute action (ACK, NACK, SCHEDULE_NACK)
     * @param view the view name
     * @param action the action to execute
     */
    suspend fun executeAction(view: String, action: Action): Lock?
}

/**
 * Event stream (locking) repository interface - get event, stream events
 */
interface EventStreamRepository {
    /**
     * Get event from the event stream
     * Enables multiple, concurrent, uncoordinated consumers to safely read/stream events per view:
     *  - It will lock the event/decider event stream/partition  for a certain amount of time (default 5 seconds)
     *  - It will return the event and the offset
     *  - Lock can be released with ACK or NACK
     *
     * @param view the view name
     */
    suspend fun getEvent(view: String): Pair<Event, Long>?

    /**
     * Stream events from the event store by pooling with a certain delay, indefinitely.
     *
     * Enables multiple, concurrent, uncoordinated consumers to safely read/stream events per view:
     *  - It will lock the event/decider event stream/partition  for a certain amount of time (default 5 seconds)
     *  - It will return the event and the offset
     *  - Lock can be released with ACK or NACK
     */
    fun streamEvents(view: String, poolingDelayMilliseconds: Long): Flow<Pair<Event, Long>>
}




