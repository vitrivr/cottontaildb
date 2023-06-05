package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.utilities.serializers.json.PathSerializer
import java.nio.file.Path
import java.nio.file.Paths

/**
 * Cottontail DB configuration class.
 *
 * @author Ralph Gasser
 * @version 1.6.2
 */
@Serializable
data class Config(
        /** Path to the root data folder used by Cottontail DB. */
        @Serializable(with = PathSerializer::class)
        val root: Path = Paths.get("./data"),

        /** Path to a custom Log4j2 config file (XML). Defaults to null! */
        @Serializable(with = PathSerializer::class)
        val logConfig: Path? = null,

        /** Flag indicating whether Cottontail DB should be allowed to start even in the presence of broken indexes.*/
        val allowBrokenIndex: Boolean = true,

        /** Reference to [XodusConfig], which contains configuration regarding Xodus. */
        val xodus: XodusConfig = XodusConfig(),

        /** Reference to [MapDBConfig], which contains configuration regarding the memory usage of Cottontail DB. */
        val mapdb: MapDBConfig = MapDBConfig(),

        /** Reference to [ServerConfig], which contains configuration regarding the gRPC server. */
        val server: ServerConfig = ServerConfig(),

        /** Reference to [ExecutionConfig], which contains configuration regarding execution in Cottontail DB. */
        val execution: ExecutionConfig = ExecutionConfig(),

        /** Reference to [CacheConfig], which contains configuration regarding caches in Cottontail DB. */
        val cache: CacheConfig = CacheConfig(),

        /** Reference to [CostConfig], which contains configuration regarding  Cottontail DB's cost model. */
        val cost: CostConfig = CostConfig(),

        /** */
        val memory: MemoryConfig = MemoryConfig()
) {
        /** Returns a path to the main data folder used by Cottontail DB. */
        fun dataFolder(): Path = this.root.resolve("xodus")

        /** Returns a path to the temporary data folder used by Cottontail DB. */
        fun temporaryDataFolder(): Path = this.root.resolve("tmp")
}