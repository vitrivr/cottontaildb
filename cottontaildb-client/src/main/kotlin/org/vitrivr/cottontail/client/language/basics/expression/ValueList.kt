package org.vitrivr.cottontail.client.language.basics.expression

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*

/**
 * A list of [Literal] values [Expression]. Mainly used for IN queries.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@Serializable
@SerialName("ValueList")
class ValueList(val value: List<PublicValue>): Expression() {
    constructor(list: List<Boolean>): this( list.map { BooleanValue(it) })
    constructor(list: List<Byte>): this( list.map { ShortValue(it) })
    constructor(list: List<Short>): this( list.map { ShortValue(it) })
    constructor(list: List<Int>): this( list.map { IntValue(it) })
    constructor(list: List<Long>): this( list.map { LongValue(it) })
    constructor(list: List<Float>): this( list.map { FloatValue(it) })
    constructor(list: List<Double>): this( list.map { DoubleValue(it) })
    constructor(list: List<Date>): this( list.map { DateValue(it) })
    constructor(list: List<String>): this( list.map { StringValue(it) })
    constructor(list: List<UUID>): this( list.map { UuidValue(it) })
    constructor(list: List<BooleanArray>): this( list.map { BooleanVectorValue(it) })
    constructor(list: List<IntArray>): this( list.map { IntVectorValue(it) })
    constructor(list: List<LongArray>): this( list.map { LongVectorValue(it) })
    constructor(list: List<FloatArray>): this( list.map { FloatVectorValue(it) })
    constructor(list: List<DoubleArray>): this( list.map { DoubleVectorValue(it) })

    override fun toGrpc(): CottontailGrpc.Expression {
        val builder = CottontailGrpc.Expression.newBuilder()
        for (data in this.value) {
            builder.literalListBuilder.addLiteral(data.toGrpc())
        }
        return builder.build()
    }
}