package ch.unibas.dmi.dbis.cottontail.config

import kotlinx.serialization.*
import kotlinx.serialization.internal.StringDescriptor
import java.nio.file.Path
import java.nio.file.Paths

@Serializable
data class Config(
    @Serializable(with=PathSerializer::class) val root: Path,
    @Optional val lockTimeout: Long = 1000L,
    @Optional val executionConfig: ExecutionConfig = ExecutionConfig()
)

@Serializer(forClass = Path::class)
object PathSerializer : KSerializer<Path> {
    override val descriptor: SerialDescriptor
        get() = StringDescriptor.withName("Path")

    override fun deserialize(input: Decoder): Path = Paths.get(input.decodeString())

    override fun serialize(output: Encoder, obj: Path) {
        output.encodeString(obj.toString())
    }
}