package org.vitrivr.cottontail.client.language.basics.expression

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.client.language.extensions.parseFunction
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [Function] [Expression] used to define a query.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
@SerialName("Function")
class Function(val name: String, vararg val args: Expression): Expression() {
    override fun toGrpc(): CottontailGrpc.Expression {
        val function = CottontailGrpc.Function.newBuilder().setName(name.parseFunction())
        for (exp in this.args) {
            function.addArguments(exp.toGrpc())
        }
        return CottontailGrpc.Expression.newBuilder().setFunction(function).build()
    }
}