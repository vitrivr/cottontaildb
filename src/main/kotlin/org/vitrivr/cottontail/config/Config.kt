package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.apache.logging.log4j.Level
import org.vitrivr.cottontail.utilities.serializers.LogLevelSerializer
import org.vitrivr.cottontail.utilities.serializers.PathSerializer
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

        /** Flag indicating whether Cottontail DB should be allowed to start even in the presence of broken indexes.*/
        val allowBrokenIndex: Boolean = true,

        /** If debug mode is no and thus stack traces should be printed. */
        @Serializable(with = LogLevelSerializer::class)
        val logLevel: Level = Level.OFF,

        /** Reference to [ServerConfig], which contains configuration regarding the gRPC server. */
        val server: ServerConfig = ServerConfig(),

        /** Reference to [MapDBConfig], which contains configuration regarding the memory usage of Cottontail DB. */
        val mapdb: MapDBConfig = MapDBConfig(),

        /** Reference to [ExecutionConfig], which contains configuration regarding query execution in Cottontail DB. */
        val execution: ExecutionConfig = ExecutionConfig()
) {

        /** True if Cottontail DB has been started in DEBUG mode. */
        val debug: Boolean
                get() = this.logLevel == Level.DEBUG

}