package org.vitrivr.cottontail.client.language.basics.expression

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A list of [Literal] values [Expression]. Mainly used for IN queries.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
@SerialName("List")
class List(val value: Array<Value>): Expression() {
    override fun toGrpc(): CottontailGrpc.Expression {
        val builder = CottontailGrpc.Expression.newBuilder()
        for (data in this.value) {
            builder.literalListBuilder.addLiteral(data.toGrpc())
        }
        return builder.build()
    }
}