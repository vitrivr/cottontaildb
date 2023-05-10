package org.vitrivr.cottontail.client.language.basics.expression

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [Function] [Expression] used to define a query.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
@SerialName("Function")
class Function(val name: Name.FunctionName, vararg val args: Expression): Expression() {

    constructor(name: String, vararg args: Expression): this(Name.FunctionName.parse(name), *args)

    override fun toGrpc(): CottontailGrpc.Expression {
        val function = CottontailGrpc.Function.newBuilder().setName(this.name.proto())
        for (exp in this.args) {
            function.addArguments(exp.toGrpc())
        }
        return CottontailGrpc.Expression.newBuilder().setFunction(function).build()
    }
}