package ch.unibas.dmi.dbis.cottontail.config

import ch.unibas.dmi.dbis.cottontail.utilities.serializers.PathSerializer
import kotlinx.serialization.*
import org.mapdb.volume.MappedFileVol
import java.nio.file.Path

@Serializable
data class Config(
    @Serializable(with= PathSerializer::class) val root: Path,
    val forceUnmapMappedFiles: Boolean = false,
    val lockTimeout: Long = 1000L,
    val serverConfig: ServerConfig = ServerConfig(),
    val executionConfig: ExecutionConfig = ExecutionConfig()
) {
    /**
     * Getter for [MappedFileVol.MappedFileFactory].
     */
    val volumeFactory
        get() = MappedFileVol.MappedFileFactory(this.forceUnmapMappedFiles, false)
}