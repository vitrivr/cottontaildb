package org.vitrivr.cottontail.client.language.basics.predicate

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A boolean OR operator, which can be used as [Predicate].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
@SerialName("Or")
data class Or(val p1: Predicate, val p2: Predicate): Predicate() {
    override fun toGrpc(): CottontailGrpc.Predicate = CottontailGrpc.Predicate.newBuilder().setOr(
        CottontailGrpc.Predicate.Or.newBuilder().setP1(this.p1.toGrpc()).setP2(this.p2.toGrpc())
    ).build()
}