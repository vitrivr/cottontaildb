package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.utilities.serializers.json.PathSerializer
import java.nio.file.Path
import java.nio.file.Paths
import java.util.UUID

/**
 * Cottontail DB configuration class.
 *
 * @author Ralph Gasser
 * @author Florian Burkhardt
 * @version 1.7.0
 */
@Serializable
data class Config(
        /** Path to the root data folder used by Cottontail DB. */
        @Serializable(with = PathSerializer::class)
        val root: Path = Paths.get("./data"),

        /** Path to a custom Log4j2 config file (XML). Defaults to null! */
        @Serializable(with = PathSerializer::class)
        val logConfig: Path? = null,

        /** Reference to [XodusConfig], which contains configuration regarding Xodus. */
        val xodus: XodusConfig = XodusConfig(),

        /** Reference to [ServerConfig], which contains configuration regarding the gRPC server. */
        val server: ServerConfig = ServerConfig(),

        /** Reference to [ExecutionConfig], which contains configuration regarding execution in Cottontail DB. */
        val execution: ExecutionConfig = ExecutionConfig(),

        /** Reference to [CacheConfig], which contains configuration regarding caches in Cottontail DB. */
        val cache: CacheConfig = CacheConfig(),

        /** Reference to [CostConfig], which contains configuration regarding Cottontail DB's cost model. */
        val cost: CostConfig = CostConfig(),

        /** Reference to [StatisticsConfig], which contains configuration regarding Cottontail DB's statistic manager. */
        val statistics: StatisticsConfig = StatisticsConfig(),

        /** Reference to [MemoryConfig], which contains configuration of memory use.*/
        val memory: MemoryConfig = MemoryConfig()
) {
        /** Returns a path to the main data folder used by Cottontail DB. */
        fun catalogueFolder(): Path = this.root.resolve("catalogue")

        /** Returns a path to the main data folder used by Cottontail DB. */
        fun dataFolder(uuid: UUID): Path = this.root.resolve("data").resolve(uuid.toString())

        /** Returns a path to the statistics data folder used by Cottontail DB. */
        fun statisticsFolder(): Path = this.root.resolve("statistics")

        /** Returns a path to the temporary data folder used by Cottontail DB. */
        fun temporaryDataFolder(): Path = this.root.resolve("tmp")
}