package com.fraktalio.example.fmodelspringdemo.adapter.eventstream

import com.fraktalio.example.fmodelspringdemo.SpringCoroutineScope
import com.fraktalio.example.fmodelspringdemo.adapter.deciderId
import com.fraktalio.example.fmodelspringdemo.application.MaterializedViewState
import com.fraktalio.example.fmodelspringdemo.domain.Event
import com.fraktalio.fmodel.application.MaterializedView
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


interface ViewRepository {
    suspend fun registerView(
        view: String,
        poolingDelayMilliseconds: Long = 500L,
        startAt: LocalDateTime = LocalDateTime.of(1, Month.JANUARY, 1, 1, 1)
    ): View

    fun findAll(): Flow<View>
    suspend fun findById(view: String): View?
}

interface LockRepository {
    fun findAll(): Flow<Lock>
    suspend fun executeAction(view: String, action: Action): Lock?
}

interface EventStreamRepository {
    suspend fun getEvent(view: String): Pair<Event, Long>?
    fun streamEvents(view: String, poolingDelayMilliseconds: Long): Flow<Pair<Event, Long>>
}

private val logger = KotlinLogging.logger {}

@OptIn(ExperimentalCoroutinesApi::class)
private val eventStreamDispatcher = Dispatchers.IO.limitedParallelism(10)

class EventStream(
    private val eventStreamingRepository: EventStreamRepository,
    private val lockRepository: LockRepository,
    private val viewRepository: ViewRepository,
    private val materializedView: MaterializedView<MaterializedViewState, Event?>
) : SpringCoroutineScope by SpringCoroutineScope(eventStreamDispatcher) {


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

    @EventListener
    fun onApplicationEvent(event: ContextRefreshedEvent?) {
        logger.info { "Spring ContextRefreshedEvent just occurred: $event" }
        launch {
            val actions = Channel<Action>()
            streamEvents("view", actions.receiveAsFlow())
                .onStart {
                    logger.info { "starting event streaming for the view 'view' ..." }
                    launch {
                        actions.send(Ack(-1, "start"))
                    }
                }
                .onEach {
                    try {
                        logger.debug { "________start____________" }
                        logger.debug { "event ingested" }
                        materializedView.handle(it.first)
                        actions.send(Ack(it.second, it.first.deciderId()))
                        logger.debug { "event processed with success / sending ACK (event: $it)" }
                        logger.debug { "---------end-----------" }
                    } catch (e: Exception) {
                        logger.error { "event processed with error / sending NACK (event: $it)" }
                        logger.error(e) { e.message }
                        logger.error { "---------end-----------" }
                        actions.send(ScheduleNack(10000, it.first.deciderId()))
                    }
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
}






