package org.vitrivr.cottontail.client.language.basics.expression

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [Column] [Expression].
 */
@Serializable
@SerialName("Column")
data class Column(val name: Name.ColumnName): Expression() {
    constructor(name: String): this(Name.ColumnName.parse(name))
    override fun toGrpc(): CottontailGrpc.Expression = CottontailGrpc.Expression.newBuilder().setColumn(this.name.proto()).build()
}