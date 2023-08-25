package studio.hcmc.ktor.plugin

import io.ktor.server.application.*
import io.lettuce.core.ClientOptions
import io.lettuce.core.ExperimentalLettuceCoroutinesApi
import io.lettuce.core.RedisClient
import io.lettuce.core.RedisURI
import io.lettuce.core.api.StatefulRedisConnection
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.support.ConnectionPoolSupport
import kotlinx.serialization.InternalSerializationApi
import org.apache.commons.pool2.impl.GenericObjectPool
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import studio.hcmc.kotlin.protocol.Id
import studio.hcmc.kotlin.protocol.IdHolder
import kotlin.reflect.KClass
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

internal typealias RedisConnection<Element, ElementId> = StatefulRedisConnection<ElementId, Element>
internal typealias RedisConnectionPool<Element, ElementId> = GenericObjectPool<RedisConnection<Element, ElementId>>
internal typealias RedisConnectionPoolConfig<Element, ElementId> = GenericObjectPoolConfig<RedisConnection<Element, ElementId>>

class RedisConfiguration<Element : IdHolder<ElementId, IdValue>, ElementId : Id<IdValue>, IdValue> {
    var host: String = "localhost"
    var port: Int = 6379
    var clientOptionsConfiguration: ClientOptions.Builder.() -> Unit = {}
    var redisCodec: RedisCodec<ElementId, Element>? = null
    var connectionSupplier: RedisClient.(RedisCodec<ElementId, Element>) -> RedisConnection<Element, ElementId> = RedisClient::connect
    var connectionPoolConfiguration: RedisConnectionPoolConfig<Element, ElementId>.() -> Unit = {}
    var elementExpireDuration: Duration = 3600.seconds
    var addOperationMaxDuration: Duration? = null
    var setOperationMaxDuration: Duration? = null
    var removeOperationMaxDuration: Duration? = null
    var getOperationMaxDuration: Duration? = null
}

@InternalSerializationApi
@ExperimentalLettuceCoroutinesApi
@Suppress("FunctionName")
fun <Element : IdHolder<ElementId, IdValue>, ElementId : Id<IdValue>, IdValue : Any> Redis(
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
        RedisConnectionPoolConfig<Element, ElementId>().apply { pluginConfig.connectionPoolConfiguration(this) }
    )

    RedisHandler[elementClass] = RedisHandler(
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
inline fun <reified Element : IdHolder<ElementId, IdValue>, reified ElementId : Id<IdValue>, reified IdValue : Any> Redis() = Redis(
    elementClass = Element::class,
    elementIdClass = ElementId::class,
    idValueClass = IdValue::class
)
