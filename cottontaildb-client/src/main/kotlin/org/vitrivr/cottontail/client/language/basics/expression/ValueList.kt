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
class ValueList(val value: Array<PublicValue>): Expression() {
    constructor(list: List<Any>): this(when (list.first()) {
        is Boolean -> list.filterIsInstance<Boolean>().map { BooleanValue(it) }
        is Byte -> list.filterIsInstance<Byte>().map { ByteValue(it) }
        is Short -> list.filterIsInstance<Short>().map { ShortValue(it) }
        is Int -> list.filterIsInstance<Int>().map { IntValue(it) }
        is Long -> list.filterIsInstance<Long>().map { LongValue(it) }
        is Float -> list.filterIsInstance<Float>().map { FloatValue(it) }
        is Double -> list.filterIsInstance<Double>().map { DoubleValue(it) }
        is Date -> list.filterIsInstance<Date>().map { DateValue(it) }
        is String -> list.filterIsInstance<String>().map { StringValue(it) }
        is UUID -> list.filterIsInstance<UUID>().map { UuidValue(it) }
        is BooleanArray -> list.filterIsInstance<BooleanArray>().map { BooleanVectorValue(it) }
        is IntArray -> list.filterIsInstance<IntArray>().map { IntVectorValue(it) }
        is LongArray -> list.filterIsInstance<LongArray>().map { LongVectorValue(it) }
        is FloatArray -> list.filterIsInstance<FloatArray>().map { FloatVectorValue(it) }
        is DoubleArray -> list.filterIsInstance<DoubleArray>().map { DoubleVectorValue(it) }
        is PublicValue -> list.filterIsInstance<PublicValue>()
        else -> throw IllegalArgumentException("Cannot create ValueList from list of type ${list.first().javaClass.simpleName}.")
    }.toTypedArray())

    override fun toGrpc(): CottontailGrpc.Expression {
        val builder = CottontailGrpc.Expression.newBuilder()
        for (data in this.value) {
            builder.literalListBuilder.addLiteral(data.toGrpc())
        }
        return builder.build()
    }
}