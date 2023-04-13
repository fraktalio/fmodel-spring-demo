package com.fraktalio.example.fmodelspringdemo.adapter.eventstream.repository

import com.fraktalio.example.fmodelspringdemo.adapter.eventstream.EventStreamRepository
import com.fraktalio.example.fmodelspringdemo.adapter.persistence.eventMapper
import com.fraktalio.example.fmodelspringdemo.domain.Event
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.isActive
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.json.Json
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.awaitSingleOrNull

private const val getEvent = """
          SELECT * FROM stream_events(:view)
        """


class EventStreamRepositoryImpl(private val databaseClient: DatabaseClient) : EventStreamRepository {
    override suspend fun getEvent(view: String): Pair<Event, Long>? =
        databaseClient.sql(getEvent)
            .bind("view", view)
            .map(eventMapper)
            .awaitSingleOrNull()
            ?.let { Pair(Json.decodeFromString(it.data.decodeToString()), it.offset ?: -1) }

    @OptIn(ExperimentalCoroutinesApi::class)
    override fun streamEvents(view: String, poolingDelayMilliseconds: Long): Flow<Pair<Event, Long>> =
        channelFlow {
            while (!isClosedForSend && isActive) {
                getEvent(view)?.let { send(it) }
                delay(poolingDelayMilliseconds)
            }
        }
}




