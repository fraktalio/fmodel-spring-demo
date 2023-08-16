package com.fraktalio.example.fmodelspringdemo.adapter.eventstream.repository

import com.fraktalio.example.fmodelspringdemo.adapter.eventstream.EventStreamRepository
import com.fraktalio.example.fmodelspringdemo.adapter.persistence.eventMapper
import com.fraktalio.example.fmodelspringdemo.domain.Event
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.awaitSingleOrNull

private const val getEvent = """
          SELECT * FROM stream_events(:view)
        """

private val logger = KotlinLogging.logger {}

class EventStreamRepositoryImpl(private val databaseClient: DatabaseClient) : EventStreamRepository {
    override suspend fun getEvent(view: String): Pair<Event, Long>? =
        databaseClient.sql(getEvent)
            .bind("view", view)
            .map(eventMapper)
            .awaitSingleOrNull()
            ?.let { Pair(Json.decodeFromString(it.data.decodeToString()), it.offset ?: -1) }

    override fun streamEvents(view: String, poolingDelayMilliseconds: Long): Flow<Pair<Event, Long>> =
        flow {
            while (currentCoroutineContext().isActive) {
                logger.debug { "# stream loop #: pulling the db for the view $view" }
                val event = getEvent(view)
                if (event != null) {
                    logger.debug { "# stream loop #: emitting the event $event" }
                    emit(event)
                    logger.debug { "# stream loop #: event emitted" }
                } else {
                    logger.debug { "# stream loop #: scheduling new pool in $poolingDelayMilliseconds milliseconds" }
                    delay(poolingDelayMilliseconds)
                }
            }
        }
}




