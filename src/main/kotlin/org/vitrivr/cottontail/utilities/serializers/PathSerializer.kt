package org.vitrivr.cottontail.utilities.serializers

import kotlinx.serialization.*
import kotlinx.serialization.internal.StringDescriptor
import java.nio.file.Path
import java.nio.file.Paths

@Serializer(forClass = Path::class)
object PathSerializer : KSerializer<Path> {
    override val descriptor: SerialDescriptor = StringDescriptor
    override fun deserialize(decoder: Decoder): Path = Paths.get(decoder.decodeString())
    override fun serialize(encoder: Encoder, obj: Path) {
        encoder.encodeString(obj.toString())
    }
}