package io.github.herromich.kotlinflow

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.flow.*

fun <T, K> Flow<T>.groupBy(getKey: (T) -> K): Flow<Pair<K, Flow<T>>> = flow {
    val storage = mutableMapOf<K, SendChannel<T>>()
    onEach { t ->
        val key = getKey(t)
        storage.getOrPut(key) {
            Channel<T>(32).also { emit(key to it.consumeAsFlow()) }
        }.send(t)
    }.onCompletion {
        storage.values.forEach { chan -> chan.close() }
    }.collect()
}

fun <T> Flow<T>.buffer(length: Int): Flow<List<T>> {
    assert(length > 0) { "Length should be greater than 0." }
    return flow {
        val list = mutableListOf<T>()
        onEach { t ->
            list += t
            if (list.size >= length) {
                val res = list.subList(0, length - 1)
                emit(res.toList())
                res.clear()
            }
        }.onCompletion {
            emit(list.toList())
        }.collect()
    }
}

fun <T, P> Flow<T>.flatMapIterable(transform: (t: T) -> List<P>): Flow<P> = flow {
    collect { t ->
        transform(t).forEach { emit(it) }
    }
}
