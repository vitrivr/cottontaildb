package org.vitrivr.cottontail.core.tuple

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.*
import java.util.*

/**
 * A [Tuple] as returned and processed by Cottontail DB. A [Tuple] corresponds to a single row and it can hold
 * multiple values, each belonging to a different column (defined by [ColumnDef]s). A [ColumnDef] must not necessarily
 * correspond to a physical database column.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
interface Tuple {

    /** The [TupleId] of this [Tuple]. Can be updated! */
    val tupleId: TupleId

    /** [Array] of [ColumnDef]s contained in this [Tuple]. */
    val columns: Array<ColumnDef<*>>

    /** Size of this [Tuple] in terms of [ColumnDef] it encompasses. */
    val size: Int
        get() = this.columns.size

    /**
     * Creates and returns a copy of this [Tuple].
     *
     * The copy of a [Tuple] is supposed to hold its own copy of the values it holds. However, structural information,
     * such as the columns, may be shared between instances, as they are supposed to be immutable.
     *
     * @return Copy of this [Tuple].
     */
    fun copy(): Tuple

    /**
     * Returns true, if this [Tuple] contains the specified [ColumnDef] and false otherwise.
     *
     * @param column The [ColumnDef] specifying the column
     * @return True if record contains the [ColumnDef], false otherwise.
     */
    fun has(column: ColumnDef<*>): Boolean = this.columns.contains(column)

    /**
     * Returns column index of the given [ColumnDef] within this [Tuple]. Returns -1 if [ColumnDef] is not contained
     *
     * @param column The [ColumnDef] to check.
     * @return The column index or -1. of [ColumnDef] is not part of this [Tuple].
     */
    fun indexOf(column: ColumnDef<*>): Int = this.columns.indexOf(column)

    /**
     * Returns column index of the given [Name.ColumnName] within this [Tuple]. Returns -1 if [Name.ColumnName] is not contained
     *
     * @param column The [Name.ColumnName] to check.
     * @return The column index or -1. of [Name.ColumnName] is not part of this [Tuple].
     */
    fun indexOf(column: Name.ColumnName): Int = this.columns.indexOfFirst { it.name == column }

    /**
     * Returns column index of the column with the given simple name within this [Tuple]. Returns -1 if [Name.ColumnName] is not contained
     *
     * @param column The simple name to check.
     * @return The column index or -1. of name is not part of this [Tuple].
     */
    fun indexOf(column: String): Int = this.columns.indexOfFirst { it.name.simple == column }

    /**
     * Retrieves the [Value] for the specified column index from this [Tuple].
     *
     * @param index The column index for which to retrieve the value.
     * @return The value for the index.
     */
    operator fun get(index: Int): Value?

    /**
     * Retrieves the [Value] for the specified [ColumnDef] from this [Tuple].
     *
     * @param column The [ColumnDef] for which to retrieve the value.
     * @return The value for the [ColumnDef]
     */
    operator fun get(column: ColumnDef<*>): Value? = this[this.indexOf(column)]

    /**
     * Retrieves the [Value] for the specified [Name.ColumnName] from this [Tuple].
     *
     * @param column The [Name.ColumnName] for which to retrieve the value.
     * @return The value for the [Name.ColumnName]
     */
    operator fun get(column: Name.ColumnName): Value? = this[this.indexOf(column)]

    /**
     * Retrieves the [Value] for the specified simple name from this [Tuple].
     *
     * @param column The simple name for which to retrieve the value.
     * @return The value for the simple name.
     */
    operator fun get(column: String): Value? = this[this.indexOf(column)]

    /**
     * Returns the [Name.ColumnName] for the provided index.
     *
     * @return [Name.ColumnName]
     */
    fun nameForIndex(index: Int) = this.columns[index].name

    /**
     * Returns the [Name.ColumnName] for the provided index.
     *
     * @return [String]
     */
    fun fqnForIndex(index: Int) = this.columns[index].name.fqn

    /**
     * Returns the simple name for the provided index.
     *
     * @return [String]
     */
    fun simpleNameForIndex(index: Int) = this.columns[index].name.simple

    /**
     * Returns a [List] of [Types] for this [Tuple].
     *
     * @return [List] of [Types] held by this [Tuple]
     */
    fun types(): List<Types<*>> = this.columns.map { it.type }

    /**
     * Returns a [Types] for the provided index.
     *
     * @return [Types]
     */
    fun type(index: Int): Types<*> = this.columns[index].type

    /**
     * Returns a [Types] for the provided [Name.ColumnName].
     *
     * @return [Types]
     */
    fun type(name: Name.ColumnName): Types<*> = this.columns[indexOf(name)].type

    /**
     * Returns a [Types] for the provided simple name.
     *
     * @return [Types]
     */
    fun type(name: String): Types<*> = this.columns[indexOf(name)].type

    /**
     * Returns a list of all [Value]s contained in this [Tuple].
     *
     * @return [List] of [Value]
     */
    fun values(): List<Value?>
    fun asBooleanValue(index: Int): BooleanValue? = this[index] as? BooleanValue
    fun asBooleanValue(name: String): BooleanValue? = this.asBooleanValue(indexOf(name))
    fun asBoolean(index: Int): Boolean? = asBooleanValue(index)?.value
    fun asBoolean(name: String): Boolean? = asBooleanValue(indexOf(name))?.value
    fun asByteValue(index: Int): ByteValue? = this[index] as? ByteValue
    fun asByteValue(name: String): ByteValue? = this.asByteValue(indexOf(name))
    fun asByte(index: Int): Byte? = asByteValue(index)?.value
    fun asByte(name: String): Byte? = asByteValue(indexOf(name))?.value
    fun asShortValue(index: Int): ShortValue? = this[index] as? ShortValue
    fun asShortValue(name: String): ShortValue? = this.asShortValue(indexOf(name))
    fun asShort(index: Int): Short? = this.asShortValue(index)?.value
    fun asShort(name: String): Short? = this.asShortValue(indexOf(name))?.value
    fun asIntValue(index: Int): IntValue? = this[index] as? IntValue
    fun asIntValue(name: String): IntValue? = this.asIntValue(indexOf(name))
    fun asInt(index: Int): Int? = this.asIntValue(index)?.value
    fun asInt(name: String): Int? = this.asIntValue(indexOf(name))?.value
    fun asLongValue(index: Int): LongValue? = this[index] as? LongValue
    fun asLongValue(name: String): LongValue? = this.asLongValue(indexOf(name))
    fun asLong(index: Int): Long? = this.asLongValue(index)?.value
    fun asLong(name: String): Long? = this.asLongValue(indexOf(name))?.value
    fun asFloatValue(index: Int): FloatValue? = this[index] as? FloatValue
    fun asFloatValue(name: String): FloatValue? = this.asFloatValue(indexOf(name))
    fun asFloat(index: Int): Float? = this.asFloatValue(index)?.value
    fun asFloat(name: String): Float? = this.asFloatValue(indexOf(name))?.value
    fun asDoubleValue(index: Int): DoubleValue? = this[index] as? DoubleValue
    fun asDoubleValue(name: String): DoubleValue? = this.asDoubleValue(indexOf(name))
    fun asDouble(index: Int): Double? = asDoubleValue(index)?.value
    fun asDouble(name: String): Double? = asDoubleValue(indexOf(name))?.value
    fun asBooleanVectorValue(index: Int): BooleanVectorValue? = this[index] as? BooleanVectorValue
    fun asBooleanVectorValue(name: String): BooleanVectorValue? = this.asBooleanVectorValue(indexOf(name))
    fun asBooleanVector(index: Int): BooleanArray? = asBooleanVectorValue(index)?.data
    fun asBooleanVector(name: String): BooleanArray? = asBooleanVectorValue(indexOf(name))?.data
    fun asIntVectorValue(index: Int): IntVectorValue? = this[index] as? IntVectorValue
    fun asIntVectorValue(name: String): IntVectorValue? = this.asIntVectorValue(indexOf(name))
    fun asIntVector(index: Int): IntArray? = asIntVectorValue(index)?.data
    fun asIntVector(name: String): IntArray? = asIntVectorValue(indexOf(name))?.data
    fun asLongVectorValue(index: Int): LongVectorValue? = this[index] as? LongVectorValue
    fun asLongVectorValue(name: String): LongVectorValue? = this.asLongVectorValue(indexOf(name))
    fun asLongVector(index: Int): LongArray? = asLongVectorValue(index)?.data
    fun asLongVector(name: String): LongArray? = asLongVectorValue(indexOf(name))?.data
    fun asFloatVectorValue(index: Int): FloatVectorValue? = this[index] as? FloatVectorValue
    fun asFloatVectorValue(name: String): FloatVectorValue? = this.asFloatVectorValue(indexOf(name))
    fun asFloatVector(index: Int): FloatArray? = asFloatVectorValue(index)?.data
    fun asFloatVector(name: String): FloatArray? = asFloatVectorValue(indexOf(name))?.data
    fun asDoubleVectorValue(index: Int): DoubleVectorValue? = this[index] as? DoubleVectorValue
    fun asDoubleVectorValue(name: String): DoubleVectorValue? = this.asDoubleVectorValue(indexOf(name))
    fun asDoubleVector(index: Int): DoubleArray? = asDoubleVectorValue(index)?.data
    fun asDoubleVector(name: String): DoubleArray? = asDoubleVectorValue(indexOf(name))?.data
    fun asStringValue(index: Int): StringValue? = this[index] as? StringValue
    fun asStringValue(name: String): StringValue? = this.asStringValue(indexOf(name))
    fun asUuidValue(index: Int): UuidValue? = this[index] as? UuidValue
    fun asUuidValue(name: String): UuidValue? = this.asUuidValue(indexOf(name))
    fun asString(index: Int): String? = this.asStringValue(index)?.value
    fun asString(name: String): String? = this.asStringValue(indexOf(name))?.value
    fun asDateValue(index: Int): DateValue? = this[index] as? DateValue
    fun asDate(index: Int): Date? = asDateValue(index)?.toDate()
    fun asDateValue(name: String): DateValue? = this.asDateValue(indexOf(name))
    fun asDate(name: String): Date? = asDateValue(name)?.toDate()
    fun asByteStringValue(index: Int): ByteStringValue? = this[index] as? ByteStringValue
    fun asByteStringValue(name: String): ByteStringValue?  = this.asByteStringValue(indexOf(name))
    fun asByteArray(index: Int): ByteArray?  = this.asByteStringValue(index)?.value
    fun asComplex32Value(index: Int): Complex32Value? = this[index] as? Complex32Value
    fun asComplex32Value(name: String): Complex32Value?  = this.asComplex32Value(indexOf(name))
    fun asComplex64Value(index: Int): Complex64Value? = this[index] as? Complex64Value
    fun asComplex64Value(name: String): Complex64Value?  = this.asComplex64Value(indexOf(name))
    fun asComplex32VectorValue(index: Int): Complex32VectorValue? = this[index] as? Complex32VectorValue
    fun asComplex64VectorValue(index: Int): Complex64VectorValue? = this[index] as? Complex64VectorValue
}