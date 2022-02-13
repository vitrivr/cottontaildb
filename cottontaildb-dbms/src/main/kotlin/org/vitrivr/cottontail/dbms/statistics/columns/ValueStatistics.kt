package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity
import java.io.ByteArrayInputStream
import java.lang.Math.floorDiv

/**
 * A basic implementation of a [ValueStatistics] object, which is used by Cottontail DB to collect and summary
 * statistics about [Value]s it encounters.
 *
 * These classes collect statistics about columns, which the query planner can use to make informed decisions
 * about how to execute a query..
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
open class ValueStatistics<T : Value>(val type: Types<T>) {

    companion object {


        /**
         * Reads a [ValueStatistics] object from a [ByteArrayInputStream].
         *
         * @param stream The [ByteArrayInputStream] to read the statistics from.
         */
        fun read(stream: ByteArrayInputStream): ValueStatistics<*> {
            val type = Types.forOrdinal(IntegerBinding.readCompressed(stream), IntegerBinding.readCompressed(stream))
            val stat = when (type) {
                Types.Complex32,
                Types.Complex64,
                is Types.Complex32Vector,
                is Types.Complex64Vector -> ValueStatistics(type)
                Types.Boolean -> BooleanValueStatistics.Binding.read(stream)
                Types.Byte -> ByteValueStatistics.Binding.read(stream)
                Types.Double -> DoubleValueStatistics.Binding.read(stream)
                Types.Float -> FloatValueStatistics.Binding.read(stream)
                Types.Int -> IntValueStatistics.Binding.read(stream)
                Types.Long -> LongValueStatistics.Binding.read(stream)
                Types.Short -> ShortValueStatistics.Binding.read(stream)
                Types.String -> StringValueStatistics.Binding.read(stream)
                Types.Date -> DateValueStatistics.Binding.read(stream)
                is Types.BooleanVector -> BooleanVectorValueStatistics.Binding.read(stream, type)
                is Types.DoubleVector -> DoubleVectorValueStatistics.Binding.read(stream, type)
                is Types.FloatVector -> FloatVectorValueStatistics.Binding.read(stream, type)
                is Types.IntVector -> IntVectorValueStatistics.Binding.read(stream, type)
                is Types.LongVector -> LongVectorValueStatistics.Binding.read(stream, type)
            }
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            return stat
        }

        /**
         * Writes a [ValueStatistics] object to a [LightOutputStream].
         *
         * @param output The [LightOutputStream] to write the statistics to.
         * @param statistics The [ValueStatistics] to write.
         */
        fun write(output: LightOutputStream, statistics: ValueStatistics<*>) {
            when (statistics) {
                is BooleanValueStatistics -> BooleanValueStatistics.Binding.write(output, statistics)
                is ByteValueStatistics -> ByteValueStatistics.Binding.write(output, statistics)
                is ShortValueStatistics -> ShortValueStatistics.Binding.write(output, statistics)
                is IntValueStatistics -> IntValueStatistics.Binding.write(output, statistics)
                is LongValueStatistics -> LongValueStatistics.Binding.write(output, statistics)
                is FloatValueStatistics -> FloatValueStatistics.Binding.write(output, statistics)
                is DoubleValueStatistics -> DoubleValueStatistics.Binding.write(output, statistics)
                is DateValueStatistics -> DateValueStatistics.Binding.write(output, statistics)
                is StringValueStatistics -> StringValueStatistics.Binding.write(output, statistics)
                is BooleanVectorValueStatistics -> BooleanVectorValueStatistics.Binding.write(output, statistics)
                is DoubleVectorValueStatistics -> DoubleVectorValueStatistics.Binding.write(output, statistics)
                is FloatVectorValueStatistics -> FloatVectorValueStatistics.Binding.write(output, statistics)
                is LongVectorValueStatistics -> LongVectorValueStatistics.Binding.write(output, statistics)
                is IntVectorValueStatistics -> IntVectorValueStatistics.Binding.write(output, statistics)
            }
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        }
    }


    /** Flag indicating that this [ValueStatistics] needs updating. */
    var fresh: Boolean = true
        protected set

    /** Number of null entries known to this [ValueStatistics]. */
    var numberOfNullEntries: Long = 0L
        protected set

    /** Number of non-null entries known to this [ValueStatistics]. */
    var numberOfNonNullEntries: Long = 0L
        protected set

    /** Total number of entries known to this [ValueStatistics]. */
    val numberOfEntries
        get() = this.numberOfNullEntries + this.numberOfNonNullEntries

    /** Smallest [Value] seen in terms of space requirement (logical size) known to this [ValueStatistics]. */
    open val minWidth: Int
        get() = this.type.logicalSize

    /** Largest [Value] in terms of space requirement (logical size) known to this [ValueStatistics] */
    open val maxWidth: Int
        get() = this.type.logicalSize

    /** Mean [Value] in terms of space requirement (logical size) known to this [ValueStatistics] */
    val avgWidth: Int
        get() = floorDiv(this.minWidth + this.maxWidth, 2)

    /**
     * Updates this [ValueStatistics] with an inserted [Value]
     *
     * @param inserted The [Value] that was deleted.
     */
    open fun insert(inserted: T?) {
        if (inserted == null) {
            this.numberOfNullEntries += 1
        } else {
            this.numberOfNonNullEntries += 1
        }
    }

    /**
     * Updates this [ValueStatistics] with a deleted [Value]
     *
     * @param deleted The [Value] that was deleted.
     */
    open fun delete(deleted: T?) {
        if (deleted == null) {
            this.numberOfNullEntries -= 1
        } else {
            this.numberOfNonNullEntries -= 1
        }
    }

    /**
     * Updates this [ValueStatistics] with a new updated value [T].
     *
     * Default implementation is simply a combination of [insert] and [delete].
     *
     * @param old The [Value] before the update.
     * @param new The [Value] after the update.
     */
    open fun update(old: T?, new: T?) {
        this.delete(old)
        this.insert(new)
    }

    /**
     * Resets this [ValueStatistics] and sets all its values to the default value.
     */
    open fun reset() {
        this.fresh = true
        this.numberOfNullEntries = 0L
        this.numberOfNonNullEntries = 0L
    }

    /**
     * Copies this [ValueStatistics] and returns it.
     *
     * @return Copy of this [ValueStatistics].
     */
    open fun copy(): ValueStatistics<T> {
        val copy = ValueStatistics(this.type)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        return copy
    }

    /**
     * Estimates [Selectivity] of the given [BooleanPredicate.Atomic], i.e., the percentage of [Record]s that match it.
     * Defaults to [Selectivity.DEFAULT_SELECTIVITY] but can be overridden by concrete implementations.
     *
     * @param predicate [BooleanPredicate.Atomic] To estimate [Selectivity] for.
     * @return [Selectivity] estimate.
     */
    open fun estimateSelectivity(predicate: BooleanPredicate.Atomic): Selectivity = Selectivity.DEFAULT_SELECTIVITY
}