package com.fraktalio.example.fmodelspringdemo.adapter.web.rest

import com.fraktalio.example.fmodelspringdemo.adapter.persistence.RestaurantCoroutineRepository
import com.fraktalio.example.fmodelspringdemo.adapter.persistence.RestaurantEntity
import com.fraktalio.example.fmodelspringdemo.domain.*
import kotlinx.coroutines.flow.*
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController
import java.util.*

@RestController
internal class AggregateRestQueryController(private val repository: RestaurantCoroutineRepository) {
    @GetMapping("restaurants")
    fun findAllRestaurants(): Flow<RestaurantEntity> = repository.findAll()

    @GetMapping("restaurants/{id}")
    suspend fun findRestaurant(@PathVariable id: String): RestaurantEntity? = repository.findById(id)
}