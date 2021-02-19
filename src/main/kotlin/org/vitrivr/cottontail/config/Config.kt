package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.utilities.serializers.PathSerializer
import java.nio.file.Files
import java.nio.file.Path

/**
 * Cottntail DB configuration class.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@Serializable
data class Config(
        /** Path to the root data folder used by Cottontail DB. */
        @Serializable(with = PathSerializer::class)
        val root: Path,

        /** Flag indicating whether to start the CLI upon starting Cottontail DB.*/
        val cli: Boolean = true,

        /** Reference to [ServerConfig], which contains configuration regarding the gRPC server. */
        val server: ServerConfig = ServerConfig(),

        /** Reference to [MapDBConfig], which contains configuration regarding the memory usage of Cottontail DB. */
        val mapdb: MapDBConfig = MapDBConfig(),

        /** Reference to [ExecutionConfig], which contains configuration regarding query execution in Cottontail DB. */
        val execution: ExecutionConfig = ExecutionConfig()
) {
        companion object {

                /**
                 * Loads and returns a [Config] from a file [Path].
                 *
                 * @param path The [Path] to read the [Config] from.
                 * @return [Config]
                 */
                fun load(path: Path) = Files.newBufferedReader(path).use {
                        Json.decodeFromString(serializer(), it.readText())
                }
        }
}