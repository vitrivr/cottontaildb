package org.vitrivr.cottontail.core.values

import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [Value] that is part of Cottontail DBs public interface.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
sealed interface PublicValue: Value {
    /**
     * Converts this [Value] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    fun toGrpc(): CottontailGrpc.Literal

}