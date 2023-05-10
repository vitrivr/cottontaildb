package org.vitrivr.cottontail.client.language.basics.predicate

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A simple [Predicate] used in a WHERE-clause of a [Query].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
sealed class Predicate {
    /**
     * Converts this [Predicate] to a gRPC representation.
     */
    abstract fun toGrpc(): CottontailGrpc.Predicate
}