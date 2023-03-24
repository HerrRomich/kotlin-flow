package io.github.herromich.kotlinflow.services

import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import mu.KotlinLogging
import java.util.concurrent.atomic.AtomicReference

private val logger = KotlinLogging.logger { }

abstract class DataProcessor<REQUEST, RESPONSE> {

    private var job: Job? = null
    private val requestChannel = Channel<RequestHolder>()
    protected abstract val dataProcessorContextName: String

    @PostConstruct
    fun init() {
        job = GlobalScope.launch {
            execute()
        }
    }

    @PreDestroy
    fun destroy() {
        job?.cancel()
    }

    private suspend fun execute() {
        flow {
            while (true) {
                emit(Unit)
                if (requestChannel.isClosedForReceive) break
            }
        }
            .flatMapMerge(1) {
                var terminator = AtomicReference<TerminalRequestHolder?>(null)
                flow {
                    while (true) {
                        val request = requestChannel.receive()
                        when (request) {
                            is TerminalRequestHolder -> {
                                terminator.set(request)
                                break
                            }

                            is IntermediateRequestHolder -> emit(request)
                            else -> throw DataProcessorException("Wrong request holder type.")
                        }
                    }
                }.processor()
                    .onEach { it.send() }
                    .onCompletion { cause ->
                        terminator.get()?.terminate()
                    }
            }.collect()
    }

    protected abstract suspend fun Flow<IntermediateRequestHolder>.processor(): Flow<ResponseHolder>

    fun <INPUT : Any, OUTPUT : Any> Flow<INPUT>.processData(
        inputProvider: (input: INPUT) -> REQUEST,
        outputProvider: (input: INPUT, response: RESPONSE) -> OUTPUT
    ): Flow<OUTPUT> {
        return flow {
            val responseChannel = Channel<ResponseHolderImpl<INPUT>>()
            val flow = onEach {
                requestChannel.send(
                    IntermediateRequestHolderImpl(
                        responseChannel = responseChannel,
                        input = it,
                        request = inputProvider(it)
                    )
                )
            }.onCompletion { cause ->
                if (cause != null) {
                    responseChannel.close()
                }
                else {
                    requestChannel.send(TerminalRequestHolder(responseChannel = responseChannel))
                }
            }
            GlobalScope.launch {
                flow.collect()
            }
            for (responseHolder in responseChannel) {
                val input = (responseHolder.input as? INPUT) ?: throw DataProcessorException("Wrong input type.")
                val response = responseHolder.response
                emit(outputProvider(input, response))
            }
        }
    }

    protected abstract inner class RequestHolder {
    }

    protected abstract inner class IntermediateRequestHolder : RequestHolder() {
        abstract val request: REQUEST
        abstract fun toResponseHolder(response: RESPONSE): ResponseHolder
    }

    private inner class IntermediateRequestHolderImpl<INPUT : Any>(
        private val responseChannel: Channel<ResponseHolderImpl<INPUT>>, val input: INPUT, override val request: REQUEST
    ) : IntermediateRequestHolder() {
        override fun toResponseHolder(response: RESPONSE) = ResponseHolderImpl(
            responseChannel = responseChannel,
            input = input,
            response = response
        )
    }

    private inner class TerminalRequestHolder(private val responseChannel: Channel<*>) :
        RequestHolder() {
        suspend fun terminate() {
            responseChannel.close()
        }
    }

    protected abstract inner class ResponseHolder {
        abstract suspend fun send()
    }

    private inner class ResponseHolderImpl<INPUT : Any>(
        private val responseChannel: Channel<ResponseHolderImpl<INPUT>>,
        val input: INPUT,
        val response: RESPONSE
    ) : ResponseHolder() {
        override suspend fun send() {
            responseChannel.trySend(this)
        }

    }
}
