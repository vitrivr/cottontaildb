package org.vitrivr.cottontail.client.language.basics.predicate

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A boolean AND operator, which can be used as [Predicate].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@Serializable
@SerialName("And")
data class And(val p1: Predicate, val p2: Predicate): Predicate() {
    override fun toGrpc(): CottontailGrpc.Predicate = CottontailGrpc.Predicate.newBuilder().setAnd(
        CottontailGrpc.Predicate.And.newBuilder().setP1(this.p1.toGrpc()).setP2(this.p2.toGrpc())
    ).build()
}