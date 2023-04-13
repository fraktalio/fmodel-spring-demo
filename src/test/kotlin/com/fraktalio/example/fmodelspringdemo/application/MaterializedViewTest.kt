package com.fraktalio.example.fmodelspringdemo.application

import com.fraktalio.example.fmodelspringdemo.domain.*
import com.fraktalio.fmodel.application.publishTo
import kotlinx.collections.immutable.toImmutableList
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestConstructor
import org.springframework.test.context.TestConstructor.AutowireMode.ALL
import java.math.BigDecimal


@SpringBootTest
@TestConstructor(autowireMode = ALL)
class MaterializedViewTest(private val materializedView: OrderRestaurantMaterializedView) {
    private val restaurantId = RestaurantId()
    private val restaurantName = RestaurantName("ce-vap")
    private val restaurantMenu: RestaurantMenu = RestaurantMenu(
        listOf(MenuItem(MenuItemId("item1"), MenuItemName("menuItemName"), Money(BigDecimal.TEN))).toImmutableList()
    )
    private val orderId = OrderId()
    private val orderLineItems = listOf(
        OrderLineItem(
            OrderLineItemId("1"),
            OrderLineItemQuantity(1),
            MenuItemId("item1"),
            MenuItemName("menuItemName")
        )
    ).toImmutableList()

    @OptIn(ExperimentalCoroutinesApi::class)
    @Test
    fun testMaterializedView(): Unit = runTest {
        val restaurantCreatedEvent = RestaurantCreatedEvent(restaurantId, restaurantName, restaurantMenu)
        val expectedState1 = MaterializedViewState(
            RestaurantViewState(restaurantId, restaurantName, restaurantMenu), null
        )
        val restaurantMenuChangedEvent = RestaurantMenuChangedEvent(
            restaurantId, restaurantMenu.copy(cuisine = RestaurantMenuCuisine.SERBIAN)
        )
        val expectedState2 = MaterializedViewState(
            RestaurantViewState(
                restaurantId,
                restaurantName,
                restaurantMenu.copy(cuisine = RestaurantMenuCuisine.SERBIAN)
            ), null
        )
        val orderCreatedEvent = OrderCreatedEvent(
            orderId,
            orderLineItems,
            restaurantId,
        )

        val expectedState3 = MaterializedViewState(
            null,
            OrderViewState(orderId, restaurantId, OrderStatus.CREATED, orderLineItems)
        )


        val states =
            flowOf(restaurantCreatedEvent, restaurantMenuChangedEvent, orderCreatedEvent).publishTo(materializedView)
                .toList()


        assertEquals(3, states.size)
        assertEquals(expectedState1, states[0])
        assertEquals(expectedState2, states[1])
        assertEquals(expectedState3, states[2])
    }
}
