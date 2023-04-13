package com.fraktalio.example.fmodelspringdemo.domain

import kotlinx.collections.immutable.toImmutableList
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test

class RestaurantSagaTest {
    private val restaurantSaga = restaurantSaga()
    private val orderId = OrderId()
    private val restaurantId = RestaurantId()
    private val orderLineItems = listOf(
        OrderLineItem(
            OrderLineItemId("1"),
            OrderLineItemQuantity(1),
            MenuItemId("item1"),
            MenuItemName("menuItemName")
        )
    ).toImmutableList()


    @Test
    fun testOrderCreatedEvent(): Unit = runBlocking {
        val orderCreatedEvent = OrderCreatedEvent(
            orderId,
            orderLineItems,
            restaurantId
        )

        with(restaurantSaga) {
            whenActionResult(
                orderCreatedEvent
            ) expectActions emptyList()

        }
    }

}