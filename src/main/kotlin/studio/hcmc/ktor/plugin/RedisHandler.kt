package studio.hcmc.ktor.plugin

import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import studio.hcmc.kotlin.protocol.Id
import io.lettuce.core.api.coroutines
import io.lettuce.core.api.coroutines.RedisKeyCoroutinesCommands
import io.lettuce.core.api.coroutines.RedisStringCoroutinesCommands
import java.time.Duration
import kotlin.reflect.KClass

@ExperimentalLettuceCoroutinesApi
open class RedisHandler<Element : Any, ElementId : Id<IdValue>, IdValue> internal constructor(
    private val redisConnectionPool: RedisConnectionPool<Element, ElementId, IdValue>,
    private val expireSeconds: Long,
    private val addMaxDuration: Duration?,
    private val setMaxDuration: Duration?,
    private val removeMaxDuration: Duration?,
    private val getMaxDuration: Duration?
) {
    companion object {
        private val handlers = HashMap<KClass<*>, RedisHandler<*, *, *>>()

        @Suppress("UNCHECKED_CAST")
        operator fun <Element : Any, ElementId : Id<IdValue>, IdValue> get(kClass: KClass<Element>): RedisHandler<Element, ElementId, IdValue> {
            return (handlers[kClass] ?: throw IllegalStateException("Redis handler for class ${kClass.qualifiedName} is not registered.")) as RedisHandler<Element, ElementId, IdValue>
        }

        internal operator fun <Element : Any, ElementId : Id<IdValue>, IdValue> set(kClass: KClass<Element>, handler: RedisHandler<Element, ElementId, IdValue>) {
            handlers[kClass] = handler
        }
    }

    /**
     * @see RedisStringCoroutinesCommands.setex
     */
    suspend fun add(key: ElementId, value: Element): String? {
        return borrow(addMaxDuration).use {
            it.coroutines().setex(key, expireSeconds, value)
        }
    }

    /**
     * @see RedisStringCoroutinesCommands.setex
     */
    suspend fun set(key: ElementId, value: Element): String? {
        return borrow(setMaxDuration).use {
            it.coroutines().setex(key, expireSeconds, value)
        }
    }

    /**
     * @see RedisKeyCoroutinesCommands.del
     */
    suspend fun remove(key: ElementId): Long? {
        return borrow(removeMaxDuration).use {
            it.coroutines().del(key)
        }
    }

    /**
     * @see RedisStringCoroutinesCommands.get
     */
    suspend fun get(key: ElementId): Element? {
        return borrow(getMaxDuration).use {
            it.coroutines().get(key)
        }
    }

    private fun borrow(duration: Duration?): RedisConnection<Element, ElementId, IdValue> {
        return redisConnectionPool.borrowObject(duration ?: redisConnectionPool.maxWaitDuration)
    }
}