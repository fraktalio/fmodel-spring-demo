package com.fraktalio.example.fmodelspringdemo.adapter.persistence

import com.fraktalio.example.fmodelspringdemo.adapter.decider
import com.fraktalio.example.fmodelspringdemo.adapter.deciderId
import com.fraktalio.example.fmodelspringdemo.adapter.event
import com.fraktalio.example.fmodelspringdemo.application.AggregateEventRepository
import com.fraktalio.example.fmodelspringdemo.domain.Command
import com.fraktalio.example.fmodelspringdemo.domain.Event
import io.r2dbc.postgresql.codec.Json.*
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import mu.KotlinLogging
import org.springframework.data.relational.core.mapping.Table
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.awaitSingle
import org.springframework.r2dbc.core.awaitSingleOrNull
import org.springframework.r2dbc.core.bind
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.transactional
import java.time.OffsetDateTime
import java.util.*

private val logger = KotlinLogging.logger {}

/**
 * Implementation of the [AggregateEventRepository].
 *
 * @author Иван Дугалић / Ivan Dugalic / @idugalic
 */
class AggregateEventRepositoryImpl(
    private val databaseClient: DatabaseClient,
    private val operator: TransactionalOperator
) : AggregateEventRepository {

    @OptIn(ExperimentalCoroutinesApi::class)
    private val dbDispatcher = Dispatchers.IO.limitedParallelism(10)

    private fun getEvents(deciderId: String): Flow<EventEntity> =
        databaseClient.sql(
            """
            SELECT * FROM get_events(:deciderId)
            """
        )
            .bind("deciderId", deciderId)
            .map(eventMapper)
            .all()
            .asFlow()


    private suspend fun getLastEvent(deciderId: String): EventEntity? =
        databaseClient.sql(
            """
            SELECT * FROM get_last_event(:deciderId)
            """
        )
            .bind("deciderId", deciderId)
            .map(eventMapper)
            .awaitSingleOrNull()

    private suspend fun append(eventEntity: EventEntity): EventEntity =
        with(eventEntity) {
            databaseClient.sql(
                """
                SELECT * FROM append_event(:event, :eventId, :decider, :deciderId, :data, :commandId, :previousId)
                """
            )
                .bind("event", event)
                .bind("eventId", eventId)
                .bind("decider", decider)
                .bind("deciderId", deciderId)
                .bind("data", of(data))
                .bind("commandId", commandId)
                .bind("previousId", previousId)
                .map(eventMapper)
                .awaitSingle()
        }


    private fun Flow<EventEntity>.appendAll(): Flow<EventEntity> = map { append(it) }.transactional(operator)


    /**
     * Fetching the current state as a series/flow of Events
     */
    override fun Command?.fetchEvents(): Flow<Pair<Event, UUID?>> =
        when (this) {
            is Command -> getEvents(deciderId()).map { it.toEventWithId() }
            null -> emptyFlow()
        }
            .onStart { logger.debug { "fetchEvents(${this@fetchEvents}) started ..." } }
            .onEach { logger.debug { "fetched event: $it" } }
            .onCompletion {
                when (it) {
                    null -> logger.debug { "fetchEvents(${this@fetchEvents}) completed successfully" }
                    else -> logger.warn { "fetchEvents(${this@fetchEvents}) completed with exception $it" }
                }
            }
            .flowOn(dbDispatcher)

    /**
     * The latest version provider
     */
    override val latestVersionProvider: (Event?) -> UUID? = { event ->
        when (event) {
            is Event -> runBlocking(dbDispatcher) { getLastEvent(event.deciderId())?.eventId }
            null -> null
        }
    }

    /**
     * Storing the new state as a series/flow of Events
     *
     * `latestVersionProvider` is used to fetch the latest version of the event stream, per need
     */
    override fun Flow<Event?>.save(latestVersionProvider: (Event?) -> UUID?): Flow<Pair<Event, UUID>> = flow {
        val previousIds: MutableMap<String, UUID?> = emptyMap<String, UUID?>().toMutableMap()
        emitAll(
            filterNotNull()
                .map {
                    previousIds.computeIfAbsent(it.deciderId()) { _ -> latestVersionProvider(it) }
                    val eventId = UUID.randomUUID()
                    val eventEntity = it.toEventEntity(eventId, previousIds[it.deciderId()])
                    previousIds[it.deciderId()] = eventId
                    eventEntity
                }
                .appendAll()
                .map { it.toEventWithId() }
        )
    }
        .onStart { logger.debug { "saving new events started ..." } }
        .onEach { logger.debug { "saving new event: $it" } }
        .onCompletion {
            when (it) {
                null -> logger.debug { "saving new events completed successfully" }
                else -> logger.warn { "saving new events completed with exception $it" }
            }
        }
        .flowOn(dbDispatcher)

    /**
     * Storing the new state as a series/flow of Events
     *
     * `latestVersion` is used to provide you with the latest known version of the state/stream
     */
    override fun Flow<Event?>.save(latestVersion: UUID?): Flow<Pair<Event, UUID>> = flow {
        var previousId: UUID? = latestVersion
        emitAll(
            filterNotNull()
                .map {
                    val eventId = UUID.randomUUID()
                    val eventEntity = it.toEventEntity(eventId, previousId)
                    previousId = eventId
                    eventEntity
                }
                .appendAll()
                .map { it.toEventWithId() }
        )
    }
        .onStart { logger.debug { "saving new events started ..." } }
        .onEach { logger.debug { "saving new event: $it" } }
        .onCompletion {
            when (it) {
                null -> logger.debug { "saving new events completed successfully" }
                else -> logger.warn { "saving new events completed with exception $it" }
            }
        }
        .flowOn(dbDispatcher)
}

@Table("events")
internal data class EventEntity(
    val decider: String,
    val deciderId: String,
    val event: String,
    val data: ByteArray,
    val eventId: UUID,
    val commandId: UUID?,
    val previousId: UUID?,
    val final: Boolean,
    val createdAt: OffsetDateTime? = null,
    val offset: Long? = null
)

internal val eventMapper: (Row, RowMetadata) -> EventEntity = { row, _ ->
    EventEntity(
        row.get("decider", String::class.java) ?: "",
        row.get("decider_id", String::class.java) ?: "",
        row.get("event", String::class.java) ?: "",
        row.get("data", ByteArray::class.java) ?: ByteArray(0),
        row.get("event_id", UUID::class.java) ?: UUID.randomUUID(),
        row.get("command_id", UUID::class.java) ?: UUID.randomUUID(),
        row.get("previous_id", UUID::class.java),
        row.get("final", String::class.java).toBoolean(),
        row.get("created_at", OffsetDateTime::class.java),
        row.get("offset", Number::class.java)?.toLong()
    )
}

internal fun EventEntity.toEventWithId() = Pair<Event, UUID>(Json.decodeFromString(data.decodeToString()), eventId)
internal fun Event.toEventEntity(eventId: UUID, previousId: UUID?, commandId: UUID? = null) = EventEntity(
    decider(),
    deciderId(),
    event(),
    Json.encodeToString(this).encodeToByteArray(),
    eventId,
    commandId,
    previousId,
    final
)