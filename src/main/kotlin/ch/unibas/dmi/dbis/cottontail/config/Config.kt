package ch.unibas.dmi.dbis.cottontail.config

import ch.unibas.dmi.dbis.cottontail.utilities.serializers.PathSerializer
import kotlinx.serialization.*
import java.nio.file.Path

@Serializable
data class Config(
    @Serializable(with= PathSerializer::class) val root: Path,
    val lockTimeout: Long = 1000L,
    val serverConfig: ServerConfig = ServerConfig(),
    val executionConfig: ExecutionConfig = ExecutionConfig()
)