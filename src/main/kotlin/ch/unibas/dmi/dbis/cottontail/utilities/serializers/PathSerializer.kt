package ch.unibas.dmi.dbis.cottontail.utilities.serializers

import kotlinx.serialization.*
import kotlinx.serialization.internal.StringDescriptor
import java.nio.file.Path
import java.nio.file.Paths

@Serializer(forClass = Path::class)
object PathSerializer : KSerializer<Path> {
    override val descriptor: SerialDescriptor
        get() = StringDescriptor.withName("Path")

    override fun deserialize(input: Decoder): Path = Paths.get(input.decodeString())

    override fun serialize(output: Encoder, obj: Path) {
        output.encodeString(obj.toString())
    }
}