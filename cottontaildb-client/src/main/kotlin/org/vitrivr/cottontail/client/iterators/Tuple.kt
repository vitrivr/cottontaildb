package org.vitrivr.cottontail.client.iterators

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.types.Types
import java.util.*

/**
 * A [Tuple] as returned by the [TupleIterator].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
abstract class Tuple(private val values: Array<PublicValue?>) {
    abstract fun nameForIndex(index: Int): String
    abstract fun simpleNameForIndex(index: Int): String
    abstract fun indexForName(name: String): Int
    abstract fun type(name: String): Types<*>
    abstract fun type(index: Int): Types<*>
    fun size() = this.values.size
    fun values(): Array<PublicValue?> = this.values
    operator fun get(name: String): PublicValue? = this.values[indexForName(name)]
    operator fun get(index: Int): PublicValue? = this.values[index]
    fun asBooleanValue(index: Int): BooleanValue? = this.values[index] as? BooleanValue
    fun asBooleanValue(name: String): BooleanValue? = this.asBooleanValue(indexForName(name))
    fun asBoolean(index: Int): Boolean? = asBooleanValue(index)?.value
    fun asBoolean(name: String): Boolean? = asBooleanValue(indexForName(name))?.value
    fun asByteValue(index: Int): ByteValue? = this.values[index] as? ByteValue
    fun asByteValue(name: String): ByteValue? = this.asByteValue(indexForName(name))
    fun asByte(index: Int): Byte? = asByteValue(index)?.value
    fun asByte(name: String): Byte? = asByteValue(indexForName(name))?.value
    fun asShortValue(index: Int): ShortValue? = this.values[index] as? ShortValue
    fun asShortValue(name: String): ShortValue? = this.asShortValue(indexForName(name))
    fun asShort(index: Int): Short? = this.asShortValue(index)?.value
    fun asShort(name: String): Short? = this.asShortValue(indexForName(name))?.value
    fun asIntValue(index: Int): IntValue? = this.values[index] as? IntValue
    fun asIntValue(name: String): IntValue? = this.asIntValue(indexForName(name))
    fun asInt(index: Int): Int? = this.asIntValue(index)?.value
    fun asInt(name: String): Int? = this.asIntValue(indexForName(name))?.value
    fun asLongValue(index: Int): LongValue? = this.values[index] as? LongValue
    fun asLongValue(name: String): LongValue? = this.asLongValue(indexForName(name))
    fun asLong(index: Int): Long? = this.asLongValue(index)?.value
    fun asLong(name: String): Long? = this.asLongValue(indexForName(name))?.value
    fun asFloatValue(index: Int): FloatValue? = this.values[index] as? FloatValue
    fun asFloatValue(name: String): FloatValue? = this.asFloatValue(indexForName(name))
    fun asFloat(index: Int): Float? = this.asFloatValue(index)?.value
    fun asFloat(name: String): Float? = this.asFloatValue(indexForName(name))?.value
    fun asDoubleValue(index: Int): DoubleValue? = this.values[index] as? DoubleValue
    fun asDoubleValue(name: String): DoubleValue? = this.asDoubleValue(indexForName(name))
    fun asDouble(index: Int): Double? = asDoubleValue(index)?.value
    fun asDouble(name: String): Double? = asDoubleValue(indexForName(name))?.value
    fun asBooleanVectorValue(index: Int): BooleanVectorValue? = this.values[index] as? BooleanVectorValue
    fun asBooleanVectorValue(name: String): BooleanVectorValue? = this.asBooleanVectorValue(indexForName(name))
    fun asBooleanVector(index: Int): BooleanArray? = asBooleanVectorValue(index)?.data
    fun asBooleanVector(name: String): BooleanArray? = asBooleanVectorValue(indexForName(name))?.data
    fun asIntVectorValue(index: Int): IntVectorValue? = this.values[index] as? IntVectorValue
    fun asIntVectorValue(name: String): IntVectorValue? = this.asIntVectorValue(indexForName(name))
    fun asIntVector(index: Int): IntArray? = asIntVectorValue(index)?.data
    fun asIntVector(name: String): IntArray? = asIntVectorValue(indexForName(name))?.data
    fun asLongVectorValue(index: Int): LongVectorValue? = this.values[index] as? LongVectorValue
    fun asLongVectorValue(name: String): LongVectorValue? = this.asLongVectorValue(indexForName(name))
    fun asLongVector(index: Int): LongArray? = asLongVectorValue(index)?.data
    fun asLongVector(name: String): LongArray? = asLongVectorValue(indexForName(name))?.data
    fun asFloatVectorValue(index: Int): FloatVectorValue? = this.values[index] as? FloatVectorValue
    fun asFloatVectorValue(name: String): FloatVectorValue? = this.asFloatVectorValue(indexForName(name))
    fun asFloatVector(index: Int): FloatArray? = asFloatVectorValue(index)?.data
    fun asFloatVector(name: String): FloatArray? = asFloatVectorValue(indexForName(name))?.data
    fun asDoubleVectorValue(index: Int): DoubleVectorValue? = this.values[index] as? DoubleVectorValue
    fun asDoubleVectorValue(name: String): DoubleVectorValue? = this.asDoubleVectorValue(indexForName(name))
    fun asDoubleVector(index: Int): DoubleArray? = asDoubleVectorValue(index)?.data
    fun asDoubleVector(name: String): DoubleArray? = asDoubleVectorValue(indexForName(name))?.data
    fun asStringValue(index: Int): StringValue? = this.values[index] as? StringValue
    fun asStringValue(name: String): StringValue? = this.asStringValue(indexForName(name))
    fun asString(index: Int): String? = this.asStringValue(index)?.value
    fun asString(name: String): String? = this.asStringValue(indexForName(name))?.value
    fun asDateValue(index: Int): DateValue? = this.values[index] as? DateValue
    fun asDate(index: Int): Date? = asDateValue(index)?.toDate()
    fun asDateValue(name: String): DateValue? = this.asDateValue(indexForName(name))
    fun asDate(name: String): Date? = asDateValue(name)?.toDate()
    fun asByteStringValue(index: Int): ByteStringValue? = this.values[index] as? ByteStringValue
    fun asByteStringValue(name: String): ByteStringValue?  = this.asByteStringValue(indexForName(name))
    fun asByteArray(index: Int): ByteArray?  = this.asByteStringValue(index)?.value
    fun asComplex32Value(index: Int): Complex32Value? = this.values[index] as? Complex32Value
    fun asComplex32Value(name: String): Complex32Value?  = this.asComplex32Value(indexForName(name))
    fun asComplex64Value(index: Int): Complex64Value? = this.values[index] as? Complex64Value
    fun asComplex64Value(name: String): Complex64Value?  = this.asComplex64Value(indexForName(name))
    fun asComplex32VectorValue(index: Int): Complex32VectorValue? = this.values[index] as? Complex32VectorValue
    fun asComplex64VectorValue(index: Int): Complex64VectorValue? = this.values[index] as? Complex64VectorValue
    override fun toString(): String = this.values.joinToString(", ") { it?.toString() ?: "<null>" }
}