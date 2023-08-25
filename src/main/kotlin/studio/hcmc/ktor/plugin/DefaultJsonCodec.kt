package studio.hcmc.ktor.plugin

import io.lettuce.core.codec.RedisCodec
import kotlinx.serialization.InternalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.serializer
import studio.hcmc.kotlin.protocol.Id
import studio.hcmc.kotlin.protocol.IdHolder
import java.nio.ByteBuffer
import kotlin.reflect.KClass

@InternalSerializationApi
internal class DefaultJsonCodec<Element : IdHolder<ElementId, IdValue>, ElementId : Id<IdValue>, IdValue : Any>(
    private val json: Json,
    private val elementClass: KClass<Element>,
    private val elementIdClass: KClass<ElementId>,
    private val idValueClass: KClass<IdValue>
) : RedisCodec<ElementId, Element> {
    private val keySerializer by lazy { idValueClass.serializer() }
    private val valueSerializer by lazy { elementClass.serializer() }
    private val keyPrefix by lazy {
        val className = elementClass.simpleName ?: return@lazy ""
        val builder = StringBuilder()
        for ((index, c) in className.removeSuffix("Id").withIndex()) {
            if (c.isUpperCase()) {
                if (index > 0) {
                    builder.append(':')
                }
                builder.append(c.lowercaseChar())
            } else {
                builder.append(c)
            }
        }

        builder.append(':').toString()
    }

    override fun decodeKey(bytes: ByteBuffer): ElementId {
        val idString = String(bytes.array()).split(":").last()
        return Id.wrap(json.decodeFromString(keySerializer, idString), elementIdClass)
    }

    override fun decodeValue(bytes: ByteBuffer): Element {
        return json.decodeFromString(valueSerializer, String(bytes.array()))
    }

    override fun encodeValue(value: Element): ByteBuffer {
        return ByteBuffer.wrap(json.encodeToString(valueSerializer, value).toByteArray())
    }

    override fun encodeKey(key: ElementId): ByteBuffer {
        return ByteBuffer.wrap((keyPrefix + key.value.toString()).toByteArray())
    }
}