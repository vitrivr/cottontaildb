package org.vitrivr.cottontail.client.language.basics.expression

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [Column] [Expression].
 */
@Serializable
@SerialName("Column")
data class Column(val name: String): Expression() {
    override fun toGrpc(): CottontailGrpc.Expression = CottontailGrpc.Expression.newBuilder().setColumn(name.parseColumn()).build()
}