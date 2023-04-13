package com.fraktalio.example.fmodelspringdemo

import kotlinx.coroutines.*
import org.springframework.beans.factory.DisposableBean
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import kotlin.coroutines.CoroutineContext


@SpringBootApplication
class FmodelSpringDemoApplication

fun main(args: Array<String>) {
    runApplication<FmodelSpringDemoApplication>(*args)
}

abstract class AbstractSpringScope(dispatcher: CoroutineDispatcher = Dispatchers.Default, job: Job = Job()) :
    CoroutineScope by CoroutineScope(dispatcher + job), DisposableBean {
    private val job: Job
        get() = coroutineContext[Job]!!

    override fun destroy() {
        job.cancel()
    }
}

interface SpringCoroutineScope : CoroutineScope, DisposableBean {
    val job: Job
}

@Suppress("FunctionName")
fun SpringCoroutineScope(
    dispatcher: CoroutineDispatcher = Dispatchers.Default,
    job: Job = SupervisorJob()
): SpringCoroutineScope =
    SpringCoroutineScope(dispatcher + job)

@Suppress("FunctionName")
fun SpringCoroutineScope(coroutineContext: CoroutineContext): SpringCoroutineScope = object :
    SpringCoroutineScope, CoroutineScope by CoroutineScope(coroutineContext), DisposableBean {
    override val job: Job
        get() = coroutineContext[Job]!!

    override fun destroy() {
        job.cancel()
    }
}

