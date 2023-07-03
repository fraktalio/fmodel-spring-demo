package com.fraktalio.example.fmodelspringdemo.adapter.web.rest

import com.fraktalio.example.fmodelspringdemo.application.Aggregate
import com.fraktalio.example.fmodelspringdemo.domain.*
import com.fraktalio.fmodel.application.handleOptimistically
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import java.util.*

@RestController
class AggregateRestCommandController(private val aggregate: Aggregate) {

    @OptIn(FlowPreview::class)
    private fun handle(command: Command): Flow<Event?> =
        aggregate.handleOptimistically(command).map { it.first }

    @PostMapping("restaurants")
    fun createRestaurant(@RequestBody command: CreateRestaurantCommand): Flow<Event?> =
        handle(command)

    @PostMapping("restaurants/{restaurantId}/orders")
    fun placeOrder(@RequestBody command: PlaceOrderCommand, @PathVariable restaurantId: UUID): Flow<Event?> {
        require(command.identifier.value == restaurantId) {
            "Restaurant identifier must be the same as the one in the request"
        }
        return handle(command)
    }
}