package org.vitrivr.cottontail.serialization.kotlinx

import kotlinx.serialization.KSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import org.vitrivr.cottontail.core.values.UuidValue
import java.util.*

/**
 * A [KSerializer] implementation for UUIDs.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object UUIDSerializer : KSerializer<UuidValue> {
    override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("UUIDSerializer", PrimitiveKind.STRING)
    override fun deserialize(decoder: Decoder): UuidValue = UuidValue(UUID.fromString(decoder.decodeString()))
    override fun serialize(encoder: Encoder, value: UuidValue) = encoder.encodeString(value.toString())
}