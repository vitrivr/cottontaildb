package org.vitrivr.cottontail.client.language.basics.predicate

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.client.language.basics.expression.Expression
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A IS NULL operator, which can be used as [Predicate] to evaluate if an [Expression] is null.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
@SerialName("IsNull")
data class IsNull(val p: Expression): Predicate() {
    override fun toGrpc(): CottontailGrpc.Predicate = CottontailGrpc.Predicate.newBuilder().setIsnull(
        CottontailGrpc.Predicate.IsNull.newBuilder().setExp(this.p.toGrpc())
    ).build()
}