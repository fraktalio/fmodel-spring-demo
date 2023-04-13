package com.fraktalio.example.fmodelspringdemo.adapter.web.rsocket

import com.fraktalio.example.fmodelspringdemo.application.Aggregate
import com.fraktalio.example.fmodelspringdemo.domain.*
import com.fraktalio.fmodel.application.handleOptimistically
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.*
import org.springframework.messaging.handler.annotation.MessageMapping
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Controller
import java.util.*

@Controller
class AggregateRsocketCommandController(private val aggregate: Aggregate) {
    @OptIn(FlowPreview::class)
    @MessageMapping("commands")
    fun handleCommand(@Payload commands: Flow<Command>): Flow<Event?> =
        aggregate.handleOptimistically(commands).map { it.first }
}