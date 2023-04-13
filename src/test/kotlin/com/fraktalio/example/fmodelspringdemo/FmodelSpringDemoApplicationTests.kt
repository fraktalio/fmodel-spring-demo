package com.fraktalio.example.fmodelspringdemo

import com.fraktalio.example.fmodelspringdemo.application.Aggregate
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestConstructor
import org.springframework.test.context.TestConstructor.AutowireMode.ALL


@SpringBootTest
@TestConstructor(autowireMode = ALL)
class FmodelSpringDemoApplicationTests(private val aggregate: Aggregate) {

    @Test
    fun testContextLoads() {
        assertNotNull(aggregate)
    }
}
