package com.fraktalio.example.fmodelspringdemo.adapter.web.rsocket

import com.fraktalio.example.fmodelspringdemo.application.Aggregate
import com.fraktalio.example.fmodelspringdemo.domain.Command
import com.fraktalio.example.fmodelspringdemo.domain.Event
import com.fraktalio.fmodel.application.handleOptimistically
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.springframework.messaging.handler.annotation.MessageMapping
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Controller

@Controller
class AggregateRsocketCommandController(private val aggregate: Aggregate) {
    @OptIn(ExperimentalCoroutinesApi::class)
    @MessageMapping("commands")
    fun handleCommand(@Payload commands: Flow<Command>): Flow<Event?> =
        aggregate.handleOptimistically(commands).map { it.first }
}