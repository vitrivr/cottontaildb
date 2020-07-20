package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.utilities.serializers.PathSerializer
import java.nio.file.Path

@Serializable
data class Config(
        @Serializable(with = PathSerializer::class)
        val root: Path,
        /**
         * Whether to enable the CLI by default or not (dedicated, blocking thread)
         */
        val cli:Boolean,
        val lockTimeout: Long = 1000L,
        val serverConfig: ServerConfig = ServerConfig(),
        val memoryConfig: MemoryConfig = MemoryConfig(),
        val executionConfig: ExecutionConfig = ExecutionConfig()
)