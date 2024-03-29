package com.fraktalio.example.fmodelspringdemo.adapter.persistence

import com.fraktalio.example.fmodelspringdemo.adapter.decider
import com.fraktalio.example.fmodelspringdemo.adapter.deciderId
import com.fraktalio.example.fmodelspringdemo.adapter.event
import com.fraktalio.example.fmodelspringdemo.application.AggregateEventRepository
import com.fraktalio.example.fmodelspringdemo.domain.Command
import com.fraktalio.example.fmodelspringdemo.domain.Event
import io.r2dbc.postgresql.codec.Json.of
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.runBlocking
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
 * Implementation of the `Fmodel` [AggregateEventRepository] = EventLockingRepository<Command?, Event?, UUID?>.
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
        fetchEventsAndMetaData().map { Pair(it.first, it.second) }

    /**
     * Fetching the current state as a series/flow of Events and Metadata.
     * This method is implemented by Default in the EventRepository interface, but we need/want to override it here.
     */
    override fun Command?.fetchEventsAndMetaData(): Flow<Triple<Event, UUID?, Map<String, Any>>> =
        when (this) {
            is Command -> getEvents(deciderId()).map { it.toEventWithIdAndMetaData() }
                .map { Triple(it.first, it.second, mapOf("commandId" to it.third as Any)) }

            null -> emptyFlow()
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
    override fun Flow<Event?>.save(latestVersionProvider: (Event?) -> UUID?): Flow<Pair<Event, UUID>> =
        saveWithMetaData(latestVersionProvider, emptyMap()).map { Pair(it.first, it.second) }

    /**
     * Storing the new state as a series/flow of Events and Metadata
     * This method is implemented by Default in the EventRepository interface, but we need/want to override it here.
     *
     * `latestVersionProvider` is used to fetch the latest version of the event stream, per need
     */
    override fun Flow<Event?>.saveWithMetaData(
        latestVersionProvider: (Event?) -> UUID?,
        metaData: Map<String, Any>
    ): Flow<Triple<Event, UUID, Map<String, Any>>> = flow {
        val previousIds: MutableMap<String, UUID?> = emptyMap<String, UUID?>().toMutableMap()
        emitAll(
            filterNotNull()
                .map {
                    previousIds.computeIfAbsent(it.deciderId()) { _ -> latestVersionProvider(it) }
                    val eventId = UUID.randomUUID()
                    val eventEntity =
                        it.toEventEntity(eventId, previousIds[it.deciderId()], metaData["commandId"] as UUID)
                    previousIds[it.deciderId()] = eventId
                    eventEntity
                }
                .appendAll()
                .map { it.toEventWithIdAndMetaData() }
                .map { Triple(it.first, it.second, mapOf("commandId" to it.third as Any)) }
        )
    }
        .flowOn(dbDispatcher)

    /**
     * Storing the new state as a series/flow of Events
     *
     * `latestVersion` is used to provide you with the latest known version of the state/stream
     */
    override fun Flow<Event?>.save(latestVersion: UUID?): Flow<Pair<Event, UUID>> =
        saveWithMetaData(latestVersion, emptyMap()).map { Pair(it.first, it.second) }

    /**
     * Storing the new state as a series/flow of Events with metadata
     * This method is implemented by Default in the EventRepository interface, but we need/want to override it here.
     *
     *
     * `latestVersion` is used to provide you with the latest known version of the state/stream
     * `metaData` is used to provide you with the metadata
     */
    override fun Flow<Event?>.saveWithMetaData(
        latestVersion: UUID?,
        metaData: Map<String, Any>
    ): Flow<Triple<Event, UUID, Map<String, Any>>> = flow {
        var previousId: UUID? = latestVersion
        emitAll(
            filterNotNull()
                .map {
                    val eventId = UUID.randomUUID()
                    val eventEntity = it.toEventEntity(eventId, previousId, metaData["commandId"] as UUID)
                    previousId = eventId
                    eventEntity
                }
                .appendAll()
                .map { it.toEventWithIdAndMetaData() }
                .map { Triple(it.first, it.second, mapOf("commandId" to it.third as Any)) }
        )
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
    val commandId: UUID,
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
internal fun EventEntity.toEventWithIdAndMetaData() =
    Triple<Event, UUID, UUID?>(Json.decodeFromString(data.decodeToString()), eventId, commandId)

internal fun Event.toEventEntity(eventId: UUID, previousId: UUID?, commandId: UUID) = EventEntity(
    decider(),
    deciderId(),
    event(),
    Json.encodeToString(this).encodeToByteArray(),
    eventId,
    commandId,
    previousId,
    final
)