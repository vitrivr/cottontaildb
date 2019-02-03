package ch.unibas.dmi.dbis.cottontail.config

import ch.unibas.dmi.dbis.cottontail.utilities.serializers.PathSerializer
import kotlinx.serialization.Optional
import kotlinx.serialization.Serializable
import java.nio.file.Path

/**
 * Config for Cottontail DB's GRPC server.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
data class ServerConfig(
    @Optional val port: Int = 4567,
    @Optional val useTls: Boolean = false,
    @Optional val messageSize: Int = 8192,
    @Optional @Serializable(with= PathSerializer::class) val certFile: Path? = null,
    @Optional @Serializable(with= PathSerializer::class) val privateKey: Path? = null
)