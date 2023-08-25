package studio.hcmc.ktor.plugin

import io.ktor.server.application.*
import io.ktor.util.*
import io.lettuce.core.ClientOptions
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.support.ConnectionPoolSupport
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import studio.hcmc.kotlin.protocol.Id
import java.nio.ByteBuffer
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

internal typealias RedisConnection<Element, ElementId, IdValue> = StatefulRedisConnection<ElementId, Element>
internal typealias RedisConnectionPool<Element, ElementId, IdValue> = GenericObjectPool<RedisConnection<Element, ElementId, IdValue>>
internal typealias RedisConnectionPoolConfig<Element, ElementId, IdValue> = GenericObjectPoolConfig<RedisConnection<Element, ElementId, IdValue>>

class RedisConfiguration<Element : Any, ElementId : Id<IdValue>, IdValue> {
    var host: String = "localhost"
    var port: Int = 6379
    var clientOptionsConfiguration: ClientOptions.Builder.() -> Unit = {}
    var redisCodec: RedisCodec<ElementId, Element>? = null
    var connectionSupplier: RedisClient.(RedisCodec<ElementId, Element>) -> RedisConnection<Element, ElementId, IdValue> = RedisClient::connect
    var connectionPoolConfiguration: RedisConnectionPoolConfig<Element, ElementId, IdValue>.() -> Unit = {}
    var elementExpireDuration: Duration = 3600.seconds
    var addOperationMaxDuration: Duration? = null
    var setOperationMaxDuration: Duration? = null
    var removeOperationMaxDuration: Duration? = null
    var getOperationMaxDuration: Duration? = null
}

@InternalSerializationApi
private class DefaultJsonCodec<Element : Any, ElementId : Id<IdValue>, IdValue : Any>(
    private val json: Json,
    private val elementClass: KClass<Element>,
    private val elementIdClass: KClass<ElementId>,
    private val idValueClass: KClass<IdValue>
) : RedisCodec<ElementId, Element> {
    private val keySerializer by lazy { idValueClass.serializer() }
    private val valueSerializer by lazy { elementClass.serializer() }

    override fun decodeKey(bytes: ByteBuffer): ElementId {
        return Id.wrap(json.decodeFromString(keySerializer, String(bytes.array())), elementIdClass)
    }

    override fun decodeValue(bytes: ByteBuffer): Element {
        return json.decodeFromString(valueSerializer, String(bytes.array()))
    }

    override fun encodeValue(value: Element): ByteBuffer {
        return ByteBuffer.wrap(json.encodeToString(valueSerializer, value).toByteArray())
    }

    override fun encodeKey(key: ElementId): ByteBuffer {
        return ByteBuffer.wrap(json.encodeToString(keySerializer, key.value).toByteArray())
    }
}

internal fun <Element : Any, ElementId, IdValue> redisConnectionPoolKey(kClass: KClass<Element>): AttributeKey<RedisConnectionPool<Element, ElementId, IdValue>> {
    return AttributeKey("redisConnectionPool-${kClass.qualifiedName}")
}

internal inline fun <reified Element : Any, ElementId, IdValue> redisConnectionPoolKey(): AttributeKey<RedisConnectionPool<Element, ElementId, IdValue>> {
    return redisConnectionPoolKey<Element, ElementId, IdValue>(Element::class)
}

fun <Element : Any, ElementId, IdValue> Application.redisConnectionPool(kClass: KClass<Element>): RedisConnectionPool<Element, ElementId, IdValue> {
    return attributes[redisConnectionPoolKey<Element, ElementId, IdValue>(kClass)]
}

inline fun <reified Element : Any, ElementId, IdValue> Application.redisConnectionPool(): RedisConnectionPool<Element, ElementId, IdValue> {
    return redisConnectionPool<Element, ElementId, IdValue>(Element::class)
}

@InternalSerializationApi
@ExperimentalLettuceCoroutinesApi
@Suppress("FunctionName")
fun <Element : Any, ElementId : Id<IdValue>, IdValue : Any> Redis(
    elementClass: KClass<Element>,
    elementIdClass: KClass<ElementId>,
    idValueClass: KClass<IdValue>
): ApplicationPlugin<RedisConfiguration<Element, ElementId, IdValue>> = createApplicationPlugin("Redis", ::RedisConfiguration) {
    val redisClient = RedisClient.create(RedisURI.create(pluginConfig.host, pluginConfig.port)).apply {
        options = ClientOptions
            .builder()
            .apply { pluginConfig.clientOptionsConfiguration(this) }
            .build()
    }

    val redisCodec = pluginConfig.redisCodec ?: DefaultJsonCodec(application.defaultJson, elementClass, elementIdClass, idValueClass)
    val redisConnectionPool = ConnectionPoolSupport.createGenericObjectPool(
        { pluginConfig.connectionSupplier(redisClient, redisCodec) },
        RedisConnectionPoolConfig<Element, ElementId, IdValue>().apply { pluginConfig.connectionPoolConfiguration(this) }
    )

    application.attributes.put(redisConnectionPoolKey<Element, ElementId, IdValue>(elementClass), redisConnectionPool)
    RedisHandler[elementClass] = RedisHandler<Element, ElementId, IdValue>(
        redisConnectionPool = redisConnectionPool,
        expireSeconds = pluginConfig.elementExpireDuration.inWholeSeconds,
        addMaxDuration = pluginConfig.addOperationMaxDuration?.toJavaDuration(),
        setMaxDuration = pluginConfig.setOperationMaxDuration?.toJavaDuration(),
        removeMaxDuration = pluginConfig.removeOperationMaxDuration?.toJavaDuration(),
        getMaxDuration = pluginConfig.getOperationMaxDuration?.toJavaDuration()
    )
}

@InternalSerializationApi
@ExperimentalLettuceCoroutinesApi
@Suppress("FunctionName")
inline fun <reified Element : Any, reified ElementId : Id<IdValue>, reified IdValue : Any> Redis(): ApplicationPlugin<RedisConfiguration<Element, ElementId, IdValue>> = createApplicationPlugin("Redis", ::RedisConfiguration) {
    Redis(Element::class, ElementId::class, IdValue::class)
}
