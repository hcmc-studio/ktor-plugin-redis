package studio.hcmc.ktor.plugin

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import studio.hcmc.kotlin.protocol.Id
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisKeyCoroutinesCommands
import io.lettuce.core.api.coroutines.RedisStringCoroutinesCommands
import studio.hcmc.kotlin.protocol.IdHolder
import java.time.Duration
import kotlin.reflect.KClass

class RedisHandler<Element : IdHolder<ElementId, IdValue>, ElementId : Id<IdValue>, IdValue> internal constructor(
    private val redisConnectionPool: RedisConnectionPool<Element, ElementId>,
    private val expireSeconds: Long,
    private val addMaxDuration: Duration?,
    private val setMaxDuration: Duration?,
    private val removeMaxDuration: Duration?,
    private val getMaxDuration: Duration?
) {
    companion object {
        private val handlers = HashMap<KClass<*>, RedisHandler<*, *, *>>()

        @Suppress("UNCHECKED_CAST")
        operator fun <Element : IdHolder<ElementId, IdValue>, ElementId : Id<IdValue>, IdValue> get(
            kClass: KClass<Element>
        ): RedisHandler<Element, ElementId, IdValue> {
            return (handlers[kClass] ?: throw IllegalStateException("Redis handler for class ${kClass.qualifiedName} is not registered.")) as RedisHandler<Element, ElementId, IdValue>
        }

        internal operator fun <Element : IdHolder<ElementId, IdValue>, ElementId : Id<IdValue>, IdValue> set(
            kClass: KClass<Element>,
            handler: RedisHandler<Element, ElementId, IdValue>
        ) {
            handlers[kClass] = handler
        }
    }

    /**
     * @see RedisStringCoroutinesCommands.setex
     */
    @ExperimentalLettuceCoroutinesApi
    suspend fun add(value: Element): String? {
        return borrow(addMaxDuration).use {
            it.coroutines().setex(value.id, expireSeconds, value)
        }
    }

    /**
     * @see RedisStringCoroutinesCommands.setex
     */
    @ExperimentalLettuceCoroutinesApi
    suspend fun set(value: Element): String? {
        return borrow(setMaxDuration).use {
            it.coroutines().setex(value.id, expireSeconds, value)
        }
    }

    /**
     * @see RedisKeyCoroutinesCommands.del
     */
    @ExperimentalLettuceCoroutinesApi
    suspend fun remove(key: ElementId): Long? {
        return borrow(removeMaxDuration).use {
            it.coroutines().del(key)
        }
    }

    @ExperimentalLettuceCoroutinesApi
    suspend fun remove(value: Element): Long? {
        return borrow(removeMaxDuration).use {
            it.coroutines().del(value.id)
        }
    }

    /**
     * @see RedisStringCoroutinesCommands.get
     */
    @ExperimentalLettuceCoroutinesApi
    suspend fun get(key: ElementId): Element? {
        return borrow(getMaxDuration).use {
            it.coroutines().get(key)
        }
    }

    @ExperimentalLettuceCoroutinesApi
    suspend fun getOrAdd(key: ElementId, orAdd: suspend (key: ElementId) -> Element): Element {
        val present = get(key)
        if (present != null) {
            return present
        }

        val element = orAdd(key)
        add(element)

        return element
    }

    private fun borrow(duration: Duration?): RedisConnection<Element, ElementId> {
        return redisConnectionPool.borrowObject(duration ?: redisConnectionPool.maxWaitDuration)
    }
}