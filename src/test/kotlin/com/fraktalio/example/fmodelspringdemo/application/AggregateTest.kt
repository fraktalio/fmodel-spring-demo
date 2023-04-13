package com.fraktalio.example.fmodelspringdemo.application

import com.fraktalio.example.fmodelspringdemo.domain.*
import com.fraktalio.fmodel.application.publishOptimisticallyTo
import kotlinx.collections.immutable.toImmutableList
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestConstructor
import org.springframework.test.context.TestConstructor.AutowireMode.ALL
import java.math.BigDecimal


@SpringBootTest
@TestConstructor(autowireMode = ALL)
class AggregateTest(private val aggregate: Aggregate) {
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

    @OptIn(FlowPreview::class, ExperimentalCoroutinesApi::class)
    @Test
    fun testAggregate(): Unit = runTest {
        val createRestaurantCommand = CreateRestaurantCommand(restaurantId, restaurantName, restaurantMenu)
        val changeRestaurantMenuCommand =
            ChangeRestaurantMenuCommand(restaurantId, restaurantMenu.copy(cuisine = RestaurantMenuCuisine.SERBIAN))
        val placeOrderCommand = PlaceOrderCommand(restaurantId, orderId, orderLineItems)
        val markOrderAsPreparedCommand = MarkOrderAsPreparedCommand(orderId)

        val events =
            flowOf(createRestaurantCommand, changeRestaurantMenuCommand, placeOrderCommand, markOrderAsPreparedCommand)
                .publishOptimisticallyTo(aggregate)
                .map { it.first }
                .toList()

        assertEquals(5, events.size)
        assertInstanceOf(RestaurantCreatedEvent::class.java, events[0])
        assertInstanceOf(RestaurantMenuChangedEvent::class.java, events[1])
        assertInstanceOf(OrderPlacedAtRestaurantEvent::class.java, events[2])
        assertInstanceOf(OrderCreatedEvent::class.java, events[3])
        assertInstanceOf(OrderPreparedEvent::class.java, events[4])
    }
}
