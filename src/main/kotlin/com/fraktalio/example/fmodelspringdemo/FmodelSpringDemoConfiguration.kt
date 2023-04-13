package com.fraktalio.example.fmodelspringdemo

import com.fraktalio.example.fmodelspringdemo.adapter.eventstream.EventStream
import com.fraktalio.example.fmodelspringdemo.adapter.eventstream.EventStreamRepository
import com.fraktalio.example.fmodelspringdemo.adapter.eventstream.LockRepository
import com.fraktalio.example.fmodelspringdemo.adapter.eventstream.ViewRepository
import com.fraktalio.example.fmodelspringdemo.adapter.eventstream.repository.EventStreamRepositoryImpl
import com.fraktalio.example.fmodelspringdemo.adapter.eventstream.repository.LockRepositoryImpl
import com.fraktalio.example.fmodelspringdemo.adapter.eventstream.repository.ViewRepositoryImpl
import com.fraktalio.example.fmodelspringdemo.adapter.persistence.*
import com.fraktalio.example.fmodelspringdemo.application.*
import com.fraktalio.example.fmodelspringdemo.domain.*
import com.fraktalio.fmodel.application.MaterializedView
import io.r2dbc.spi.ConnectionFactory
import kotlinx.serialization.json.Json
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.convert.converter.Converter
import org.springframework.core.io.ClassPathResource
import org.springframework.data.convert.ReadingConverter
import org.springframework.data.convert.WritingConverter
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions
import org.springframework.http.codec.json.KotlinSerializationJsonDecoder
import org.springframework.http.codec.json.KotlinSerializationJsonEncoder
import org.springframework.messaging.converter.KotlinSerializationJsonMessageConverter
import org.springframework.messaging.rsocket.RSocketStrategies
import org.springframework.r2dbc.connection.init.CompositeDatabasePopulator
import org.springframework.r2dbc.connection.init.ConnectionFactoryInitializer
import org.springframework.r2dbc.connection.init.ResourceDatabasePopulator
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.web.util.pattern.PathPatternRouteMatcher


@Configuration
class FmodelSpringDemoConfiguration {
    @Bean
    fun initializer(connectionFactory: ConnectionFactory): ConnectionFactoryInitializer =
        ConnectionFactoryInitializer().apply {
            setConnectionFactory(connectionFactory)
            setDatabasePopulator(
                CompositeDatabasePopulator().apply {
                    addPopulators(
                        ResourceDatabasePopulator(ClassPathResource("./sql/event_sourcing.sql")),
                        ResourceDatabasePopulator(ClassPathResource("./sql/event_streaming.sql")),
                        ResourceDatabasePopulator(ClassPathResource("./sql/projections.sql")),
                        ResourceDatabasePopulator(ClassPathResource("./sql/data.sql"))
                    )
                }
            )
        }

    @Bean
    internal fun restaurantDeciderBean() = restaurantDecider()

    @Bean
    internal fun orderDeciderBean() = orderDecider()

    @Bean
    internal fun restaurantSagaBean() = restaurantSaga()

    @Bean
    internal fun orderSagaBean() = orderSaga()

    @Bean
    internal fun aggregateEventRepositoryBean(
        databaseClient: DatabaseClient, operator: TransactionalOperator
    ): AggregateEventRepository = AggregateEventRepositoryImpl(databaseClient, operator)

    @Bean
    internal fun aggregateBean(
        restaurantDecider: RestaurantDecider,
        orderDecider: OrderDecider,
        restaurantSaga: RestaurantSaga,
        orderSaga: OrderSaga,
        eventRepository: AggregateEventRepository
    ): Aggregate = aggregate(orderDecider, restaurantDecider, orderSaga, restaurantSaga, eventRepository)

    @Bean
    internal fun restaurantViewBean() = restaurantView()

    @Bean
    internal fun orderViewBean() = orderView()

    @Bean
    internal fun materializedViewStateRepositoryBean(
        restaurantRepository: RestaurantCoroutineRepository,
        restaurantOrderRepository: OrderCoroutineRepository,
        restaurantOrderItemRepository: OrderItemCoroutineRepository,
        menuItemCoroutineRepository: MenuItemCoroutineRepository,
        operator: TransactionalOperator
    ): MaterializedViewStateRepository =
        MaterializedViewStateRepositoryImpl(
            restaurantRepository,
            restaurantOrderRepository,
            restaurantOrderItemRepository,
            menuItemCoroutineRepository,
            operator
        )

    @Bean
    internal fun materializedViewBean(
        restaurantView: RestaurantView,
        orderView: OrderView,
        viewStateRepository: MaterializedViewStateRepository
    ) = materializedView(restaurantView, orderView, viewStateRepository)

    @Bean
    fun eventStreamingRepositoryBean(databaseClient: DatabaseClient): EventStreamRepository =
        EventStreamRepositoryImpl(databaseClient)

    @Bean
    fun lockRepositoryBean(databaseClient: DatabaseClient): LockRepository =
        LockRepositoryImpl(databaseClient)

    @Bean
    fun viewRepositoryBean(databaseClient: DatabaseClient): ViewRepository =
        ViewRepositoryImpl(databaseClient)

    @Bean
    @ConditionalOnProperty(
        prefix = "fmodel",
        name = ["eventstream.enabled"],
        havingValue = "true",
        matchIfMissing = true
    )
    fun eventStreamBean(
        eventStreamRepository: EventStreamRepository,
        lockRepository: LockRepository,
        viewRepository: ViewRepository,
        materializedView: MaterializedView<MaterializedViewState, Event?>
    ) = EventStream(eventStreamRepository, lockRepository, viewRepository, materializedView)

    @Bean
    fun messageConverter(): KotlinSerializationJsonMessageConverter {
        return KotlinSerializationJsonMessageConverter(Json)
    }

    @Bean
    fun rsocketStrategies(): RSocketStrategies {

        return RSocketStrategies.builder()
            .encoders { it.add(KotlinSerializationJsonEncoder()) }
            .decoders { it.add(KotlinSerializationJsonDecoder()) }
            .routeMatcher(PathPatternRouteMatcher())
            .build()
    }
}

@ReadingConverter
class JsonToObjectConverter : Converter<io.r2dbc.postgresql.codec.Json, ByteArray> {
    override fun convert(source: io.r2dbc.postgresql.codec.Json): ByteArray =
        source.asArray()
}

@WritingConverter
class ObjectToJsonConverter : Converter<ByteArray, io.r2dbc.postgresql.codec.Json> {
    override fun convert(source: ByteArray): io.r2dbc.postgresql.codec.Json = io.r2dbc.postgresql.codec.Json.of(source)
}

@Configuration
class FStoreReactivePostgresConfiguration(private val connectionFactory: ConnectionFactory) :
    AbstractR2dbcConfiguration() {

    override fun connectionFactory(): ConnectionFactory = connectionFactory

    @Bean
    override fun r2dbcCustomConversions(): R2dbcCustomConversions =
        R2dbcCustomConversions(
            storeConversions,
            listOf(JsonToObjectConverter(), ObjectToJsonConverter())
        )
}