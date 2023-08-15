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
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import org.springframework.context.event.ContextRefreshedEvent
import org.springframework.context.event.EventListener
import java.time.LocalDateTime
import java.time.Month
import kotlin.coroutines.cancellation.CancellationException

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

private val logger = KotlinLogging.logger {}

/**
 * Kotlin coroutine dispatcher for event stream processing, with limited parallelism.
 */
@OptIn(ExperimentalCoroutinesApi::class)
private val eventStreamDispatcher = Dispatchers.IO.limitedParallelism(10)

/**
 * Event stream subscriber/processor
 *
 * It extends [SpringCoroutineScope] to map the Spring bean lifecycle to the kotlin coroutine scope in a robust way.
 */
class EventStreamProcessor(
    private val eventStreamingRepository: EventStreamRepository,
    private val lockRepository: LockRepository,
    private val viewRepository: ViewRepository,
    private val materializedView: MaterializedView<MaterializedViewState, Event?>? = null,
    private val sagaManager: SagaManager<Event?, Command>? = null
) : SpringCoroutineScope by SpringCoroutineScope(eventStreamDispatcher) {
    /**
     * Creates a stream of events for the given view.
     * The actions flow and the resulting events flow are independent.
     *
     * @param view the view name
     * @param actions the actions to execute
     */
    private fun streamEvents(
        @Suppress("SameParameterValue") view: String,
        actions: Flow<Action>
    ): Flow<Pair<Event, Long>> =
        channelFlow {
            viewRepository.findById(view)?.let { viewEntity ->
                launch {
                    actions.collect {
                        lockRepository.executeAction(view, it)
                    }
                }
                eventStreamingRepository.streamEvents(view, viewEntity.poolingDelayMilliseconds)
                    .collect { send(it) }
            }
        }

    /**
     * Register a materialized view and start pooling events by using [streamEvents] function
     * @param view the view name
     * @param materializedView the materialized view to register - event handler
     */
    private fun registerEventHandlerAndStartPooling(
        view: String,
        materializedView: MaterializedView<MaterializedViewState, Event?>,
    ) {
        launch {
            val actions = Channel<Action>()
            streamEvents(view, actions.receiveAsFlow())
                .onStart {
                    logger.info { "starting event streaming for the view 'view' ..." }
                    launch {
                        actions.send(Ack(-1, "start"))
                    }
                }
                .onEach {
                    try {
                        logger.debug { "handling event: $it ..." }
                        materializedView.handle(it.first)
                        logger.debug { "sending ACK/SUCCESS for: $it ..." }
                        actions.send(Ack(it.second, it.first.deciderId()))
                    } catch (e: Exception) {
                        logger.debug { "sending ScheduleNack/RETRY in 10 seconds, for: $it ..." }
                        actions.send(ScheduleNack(10000, it.first.deciderId()))
                    }
                }
                .retry(5) { cause ->
                    cause !is CancellationException
                }
                .onCompletion {
                    when (it) {
                        null -> logger.info { "event stream closed successfully" }
                        else -> logger.warn(it) { "event stream closed exceptionally: $it" }
                    }
                }
                .collect()
        }
    }

    /**
     * Register a saga manager and start pooling events by using [streamEvents] function
     * @param view the view name
     * @param sagaManager the saga manager to register - event handler
     */
    private fun registerSagaManagerAndStartPooling(
        view: String,
        sagaManager: SagaManager<Event?, Command>,
    ) {
        launch {
            val actions = Channel<Action>()
            streamEvents(view, actions.receiveAsFlow())
                .onStart {
                    logger.info { "starting event streaming for the saga/view 'saga' ..." }
                    launch {
                        actions.send(Ack(-1, "start"))
                    }
                }
                .onEach {
                    try {
                        logger.debug { "handling event: $it ..." }
                        sagaManager.handle(it.first).collect()
                        logger.debug { "sending ACK/SUCCESS for: $it ..." }
                        actions.send(Ack(it.second, it.first.deciderId()))
                    } catch (e: Exception) {
                        logger.debug { "sending ScheduleNack/RETRY in 10 seconds, for: $it ..." }
                        actions.send(ScheduleNack(10000, it.first.deciderId()))
                    }
                }
                .retry(5) { cause ->
                    cause !is CancellationException
                }
                .onCompletion {
                    when (it) {
                        null -> logger.info { "event stream closed successfully" }
                        else -> logger.warn(it) { "event stream closed exceptionally: $it" }
                    }
                }
                .collect()
        }
    }

    /**
     * Register event handler and saga manager on Spring ContextRefreshedEvent
     */
    @EventListener
    fun onApplicationEvent(event: ContextRefreshedEvent?) {
        logger.info { "Spring ContextRefreshedEvent just occurred: $event" }
        materializedView?.let { registerEventHandlerAndStartPooling("view", it) }
        sagaManager?.let { registerSagaManagerAndStartPooling("saga", it) }
    }
}






