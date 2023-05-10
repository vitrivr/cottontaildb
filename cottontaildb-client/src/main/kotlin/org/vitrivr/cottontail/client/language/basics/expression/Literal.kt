package org.vitrivr.cottontail.client.language.basics.expression

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc.DoubleVector

/**
 * A [Literal] value [Expression].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
@SerialName("Literal")
data class Literal(val value: PublicValue): Expression() {
    constructor(value: Boolean): this(BooleanValue(value))
    constructor(value: Byte): this(ByteValue(value))
    constructor(value: Short): this(ShortValue(value))
    constructor(value: Int): this(IntValue(value))
    constructor(value: Long): this(LongValue(value))
    constructor(value: Float): this(FloatValue(value))
    constructor(value: Double): this(DoubleValue(value))
    constructor(value: String): this(StringValue(value))
    constructor(value: BooleanArray): this(BooleanVectorValue(value))
    constructor(value: IntArray): this(IntVectorValue(value))
    constructor(value: LongArray): this(LongVectorValue(value))
    constructor(value: FloatArray): this(FloatVectorValue(value))
    constructor(value: DoubleArray): this(DoubleVectorValue(value))

    override fun toGrpc(): CottontailGrpc.Expression {
        val expression = CottontailGrpc.Expression.newBuilder()
        expression.literal = this.value.toGrpc()
        return expression.build()
    }
}