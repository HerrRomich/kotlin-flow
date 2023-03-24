package io.github.herromich.kotlinflow

import io.github.herromich.kotlinflow.services.DataProcessor
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import org.springframework.stereotype.Service
import kotlin.random.Random

@Service
class StringDataProcessor : DataProcessor<String, String>() {
    override val dataProcessorContextName = "int-data-processor-context"

    override suspend fun Flow<IntermediateRequestHolder>.processor() = flow {
        val channel = Channel<IntermediateRequestHolder>()
        groupBy { it.hashCode() % 16 }
            .flatMapMerge(16) {
                it.second
                    .buffer(5000)
                    .map { it.map { Random.nextInt(10) to it } }
                    .onEach { kotlinx.coroutines.delay((it.size * Random.nextDouble(0.005, 0.01)).toLong()) }
                    .flatMapIterable { it }
                    .mapNotNull { (res, requestHolder) ->
                        if (res != 3) {
                            channel.send(requestHolder)
                            null
                        } else requestHolder
                    }
                    .buffer(5000)
                    .map { it.map { Random.nextInt(10) to it } }
                    .onEach { kotlinx.coroutines.delay((it.size * Random.nextDouble(0.05, 0.1)).toLong()) }
                    .flatMapIterable { it }
                    .mapNotNull { (res, requestHolder) ->
                        if (res != 7) {
                            channel.send(requestHolder)
                            null
                        } else requestHolder
                    }
                    .buffer(5000)
                    .onEach { kotlinx.coroutines.delay((it.size * Random.nextDouble(0.05, 0.1)).toLong()) }
                    .flatMapIterable { it }
            }.merge(channel)
            .onEach {
                emit(it.toResponseHolder(it.request))
            }.collect()
    }

    private suspend fun Flow<IntermediateRequestHolder>.merge(channel: Channel<IntermediateRequestHolder>) = flow {
        GlobalScope.launch {
            onEach { channel.send(it) }
                .onCompletion { channel.close() }
                .collect()
        }
        for (s in channel) {
            emit(s)
        }
    }
}
