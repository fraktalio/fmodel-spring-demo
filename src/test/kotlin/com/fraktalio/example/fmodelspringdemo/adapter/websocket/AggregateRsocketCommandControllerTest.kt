package com.fraktalio.example.fmodelspringdemo.adapter.websocket

import com.fraktalio.example.fmodelspringdemo.domain.*
import kotlinx.collections.immutable.toImmutableList
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.server.LocalServerPort
import org.springframework.messaging.rsocket.RSocketRequester
import org.springframework.messaging.rsocket.RSocketStrategies
import org.springframework.messaging.rsocket.dataWithType
import org.springframework.messaging.rsocket.retrieveFlow
import org.springframework.test.context.TestConstructor
import org.springframework.test.context.TestConstructor.AutowireMode.ALL
import reactor.core.publisher.Hooks
import java.math.BigDecimal
import java.net.URI

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@TestConstructor(autowireMode = ALL)
class AggregateRsocketCommandControllerTest(
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
    fun commandsTest(): Unit = runTest {
        Hooks.onErrorDropped { println(" On error dropped - That should not be harmful and can be disabled with `Hooks.onErrorDropped(() -> {})`: $it") }

        val rSocketRequester = rsocketBuilder
            .rsocketStrategies(rSocketStrategies)
            .websocket(URI("ws://localhost:${serverPort}"))

        val createRestaurantCommand = CreateRestaurantCommand(restaurantId, restaurantName, restaurantMenu)

        assertThat(
            rSocketRequester
                .route("commands")
                .dataWithType(flowOf<Command>(createRestaurantCommand))
                .retrieveFlow<Event>()
                .toList().size
        ).isEqualTo(1)
    }
}