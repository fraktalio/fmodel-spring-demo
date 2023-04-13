package com.fraktalio.example.fmodelspringdemo.adapter.eventstream.repository

import com.fraktalio.example.fmodelspringdemo.adapter.eventstream.View
import com.fraktalio.example.fmodelspringdemo.adapter.eventstream.ViewRepository
import io.r2dbc.spi.Row
import io.r2dbc.spi.RowMetadata
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.reactive.asFlow
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.awaitOne
import org.springframework.r2dbc.core.awaitOneOrNull
import java.time.LocalDateTime
import java.time.Month
import java.time.OffsetDateTime

@Table("views")
internal data class ViewEntity(
    @Id
    @Column("view")
    val view: String,
    @Column("pooling_delay")
    val poolingDelayMilliseconds: Long = 500L,
    @Column("start_at")
    val startAt: LocalDateTime = LocalDateTime.of(1, Month.JANUARY, 1, 1, 1),
    @Column("created_at")
    val createdAt: OffsetDateTime? = null,
    @Column("updated_at")
    val updatedAt: OffsetDateTime? = null
)

private const val registerView = """
            INSERT INTO "views"
              ("view", "pooling_delay", "start_at") VALUES (:view, :poolingDelayMilliseconds, :startAt)
              ON CONFLICT ON CONSTRAINT "views_pkey"
              DO UPDATE SET "updated_at" = NOW(), "start_at" = EXCLUDED."start_at", "pooling_delay" = EXCLUDED."pooling_delay"
              RETURNING *
        """

// View database mapper function
internal val viewMapper: (Row, RowMetadata) -> ViewEntity = { row, _ ->
    ViewEntity(
        row.get("view", String::class.java) ?: "",
        row.get("pooling_delay", Number::class.java)?.toLong() ?: -1L,
        row.get("start_at", LocalDateTime::class.java) ?: LocalDateTime.of(1, Month.JANUARY, 1, 1, 1),
        row.get("created_at", OffsetDateTime::class.java),
        row.get("updated_at", OffsetDateTime::class.java),
    )
}

internal fun ViewEntity.asView() = View(view, poolingDelayMilliseconds)

/**
 * View repository
 *
 * @constructor Create View repository
 */
class ViewRepositoryImpl(private val databaseClient: DatabaseClient) : ViewRepository {

    override suspend fun registerView(
        view: String,
        poolingDelayMilliseconds: Long,
        startAt: LocalDateTime
    ): View = databaseClient.sql(registerView)
        .bind("view", view)
        .bind("poolingDelayMilliseconds", poolingDelayMilliseconds)
        .bind("startAt", startAt)
        .map(viewMapper)
        .awaitOne()
        .asView()


    override fun findAll() = databaseClient
        .sql("select * from views")
        .map(viewMapper)
        .all()
        .asFlow()
        .map { it.asView() }

    override suspend fun findById(view: String): View? = databaseClient
        .sql("select * from views where view = :view")
        .bind("view", view)
        .map(viewMapper)
        .awaitOneOrNull()
        ?.asView()
}
