package com.fraktalio.example.fmodelspringdemo.adapter.websocket

import com.fraktalio.example.fmodelspringdemo.adapter.persistence.RestaurantEntity
import com.fraktalio.example.fmodelspringdemo.application.OrderRestaurantMaterializedView
import com.fraktalio.example.fmodelspringdemo.domain.*
import com.fraktalio.fmodel.application.handle
import kotlinx.collections.immutable.toImmutableList
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.messaging.rsocket.RSocketRequester
import org.springframework.messaging.rsocket.RSocketStrategies
import org.springframework.messaging.rsocket.retrieveAndAwaitOrNull
import org.springframework.messaging.rsocket.retrieveFlow
import org.springframework.test.context.TestConstructor
import org.springframework.test.context.TestConstructor.AutowireMode.ALL
import reactor.core.publisher.Hooks
import java.math.BigDecimal
import java.net.URI

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@TestConstructor(autowireMode = ALL)
class AggregateRsocketQueryControllerTest(
    private val materializedView: OrderRestaurantMaterializedView,
    private val rsocketBuilder: RSocketRequester.Builder,
    private val rSocketStrategies: RSocketStrategies,
    @LocalServerPort val serverPort: Int
) {

    private val restaurantId = RestaurantId()
    private val restaurantName = RestaurantName("ce-vap")
    private val restaurantMenu: RestaurantMenu = RestaurantMenu(
        listOf(MenuItem(MenuItemId("item1"), MenuItemName("menuItemName"), Money(BigDecimal.TEN))).toImmutableList()
    )

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun queriesTest(): Unit = runTest {
        Hooks.onErrorDropped { println(" On error dropped - That should not be harmful and can be disabled with `Hooks.onErrorDropped(() -> {})`: $it") }

        val rSocketRequester = rsocketBuilder
            .rsocketStrategies(rSocketStrategies)
            .websocket(URI("ws://localhost:${serverPort}"))


        materializedView.handle(RestaurantCreatedEvent(restaurantId, restaurantName, restaurantMenu))

        assertThat(
            rSocketRequester
                .route("queries.restaurants")
                .retrieveFlow<RestaurantEntity>()
                .toList().size
        ).isEqualTo(1)

        assertThat(
            rSocketRequester
                .route("queries.restaurants.${restaurantId.value}")
                .retrieveAndAwaitOrNull<RestaurantEntity>()
        ).isNotNull


    }
}