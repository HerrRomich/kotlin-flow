package io.github.herromich.kotlinflow

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import mu.KotlinLogging
import org.apache.commons.lang3.RandomStringUtils
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import java.lang.RuntimeException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

private val logger = KotlinLogging.logger { }

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Test(
    @Autowired
    val stringDataProcessor: StringDataProcessor
) {
    val counter = AtomicInteger()

    @Test
    fun test() {
        counter.set(0)
        runBlocking {
            coroutineScope {
                (0..2999).forEach {
                    launch {
                        process()
                    }
                }
            }
        }
    }

    private suspend fun process() {
        with(stringDataProcessor) {
            (0..Random.nextInt(10, 5000)).asFlow()
                .map {
                    RandomStringUtils.randomAlphanumeric(Random.nextInt(10, 100))
                }.onEach {
                    if (Random.nextInt(100000) == 347)
                        throw RuntimeException()
                }
                .processData({ v -> v }, { _, b -> b })
                .onEach {
                    val count = counter.incrementAndGet()
                    if (count % 10000 == 0) {
                        logger.info { "Processed : $count" }
                    }
                }
                .collect()
        }
    }
}

