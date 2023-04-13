package com.fraktalio.example.fmodelspringdemo.adapter.eventstream.repository

import com.fraktalio.example.fmodelspringdemo.adapter.eventstream.*
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactive.asFlow
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.awaitOneOrNull
import java.time.OffsetDateTime
import java.time.format.DateTimeFormatter

@Table("locks")
internal data class LockEntity(
    @Column("view")
    val view: String,
    @Column("decider_id")
    val deciderId: String,
    @Column("offset")
    val offset: Long = -1L,
    @Column("last_offset")
    val lastOffset: Long = 0L,
    @Column("locked_until")
    val lockedUntil: OffsetDateTime? = null,
    @Column("offset_final")
    val offsetFinal: Boolean,
    @Column("created_at")
    val createdAt: OffsetDateTime? = null,
    @Column("updated_at")
    val updatedAt: OffsetDateTime? = null,
)

internal fun LockEntity.asLock() = Lock(
    view,
    deciderId,
    offset,
    lastOffset,
    lockedUntil?.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
    offsetFinal,
    createdAt?.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME),
    updatedAt?.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
)

private const val ackEventOffset = """
          SELECT * FROM ack_event(:view, :deciderId, :offset)
        """

private const val nackEventOffset = """
          SELECT * FROM nack_event(:view, :deciderId)
        """

private const val scheduleNackEventOffset = """
          SELECT * FROM schedule_nack_event(:view, :deciderId, :milliseconds)
        """


internal val lockEntityMapper: (Row, RowMetadata) -> LockEntity = { row, _ ->
    LockEntity(
        row.get("view", String::class.java) ?: "",
        row.get("decider_id", String::class.java) ?: "",
        row.get("offset", Number::class.java)?.toLong() ?: -1L,
        row.get("last_offset", Number::class.java)?.toLong() ?: 0L,
        row.get("locked_until", OffsetDateTime::class.java),
        row.get("offset_final", String::class.java).toBoolean(),
        row.get("created_at", OffsetDateTime::class.java),
        row.get("updated_at", OffsetDateTime::class.java),
    )
}

/**
 * Lock repository
 *
 * @constructor Create View repository
 */
class LockRepositoryImpl(private val databaseClient: DatabaseClient) : LockRepository {
    override fun findAll() = databaseClient
        .sql("select * from locks")
        .map(lockEntityMapper)
        .all()
        .asFlow()
        .map { it.asLock() }

    override suspend fun executeAction(view: String, action: Action): Lock? =
        when (action) {
            is Ack -> databaseClient.sql(ackEventOffset)
                .bind("offset", action.offset)
                .bind("view", view)
                .bind("deciderId", action.deciderId)
                .map(lockEntityMapper)
                .awaitOneOrNull()
                ?.asLock()

            is Nack -> databaseClient.sql(nackEventOffset)
                .bind("view", view)
                .bind("deciderId", action.deciderId)
                .map(lockEntityMapper)
                .awaitOneOrNull()
                ?.asLock()

            is ScheduleNack -> databaseClient.sql(scheduleNackEventOffset)
                .bind("view", view)
                .bind("milliseconds", action.milliseconds)
                .bind("deciderId", action.deciderId)
                .map(lockEntityMapper)
                .awaitOneOrNull()
                ?.asLock()
        }
}
