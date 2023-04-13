package com.fraktalio.example.fmodelspringdemo.adapter.web.rsocket

import com.fraktalio.example.fmodelspringdemo.adapter.persistence.RestaurantCoroutineRepository
import com.fraktalio.example.fmodelspringdemo.adapter.persistence.RestaurantEntity
import com.fraktalio.example.fmodelspringdemo.domain.*
import kotlinx.coroutines.flow.*
import org.springframework.messaging.handler.annotation.DestinationVariable
import org.springframework.messaging.handler.annotation.MessageMapping
import org.springframework.stereotype.Controller
import java.util.*

@Controller
internal class AggregateRsocketQueryController(private val repository: RestaurantCoroutineRepository) {
    @MessageMapping("queries.restaurants")
    fun findAllRestaurants(): Flow<RestaurantEntity> = repository.findAll()

    @MessageMapping("queries.restaurants.{id}")
    suspend fun findRestaurant(@DestinationVariable id: String): RestaurantEntity? = repository.findById(id)
}