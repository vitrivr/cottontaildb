package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A basic implementation of a [ValueStatistics] object, which is used by Cottontail DB to maintain summary statistics about [Value]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
open class ValueStatistics<T : Value>(val type: Type<T>) {

    companion object : Serializer<ValueStatistics<*>> {
        override fun serialize(out: DataOutput2, value: ValueStatistics<*>) {
            out.packInt(value.type.ordinal)
            out.packInt(value.type.logicalSize)
            when (value) {
                is ByteValueStatistics -> ByteValueStatistics.serialize(out, value)
                is ShortValueStatistics -> ShortValueStatistics.serialize(out, value)
                is IntValueStatistics -> IntValueStatistics.serialize(out, value)
                is LongValueStatistics -> LongValueStatistics.serialize(out, value)
                is FloatValueStatistics -> FloatValueStatistics.serialize(out, value)
                is DoubleValueStatistics -> DoubleValueStatistics.serialize(out, value)
                is DateValueStatistics -> DateValueStatistics.serialize(out, value)
                is StringValueStatistics -> StringValueStatistics.serialize(out, value)
            }
            out.writeBoolean(value.dirty)
            out.packLong(value.numberOfNullEntries)
            out.packLong(value.numberOfNonNullEntries)
        }

        override fun deserialize(input: DataInput2, available: Int): ValueStatistics<*> {
            val stat = when (val type = Type.forOrdinal(input.unpackInt(), input.unpackInt())) {
                Type.Boolean -> BooleanValueStatistics.deserialize(input, available)
                Type.Byte -> ByteValueStatistics.deserialize(input, available)
                Type.Double -> DoubleValueStatistics.deserialize(input, available)
                Type.Float -> FloatValueStatistics.deserialize(input, available)
                Type.Int -> IntValueStatistics.deserialize(input, available)
                Type.Long -> LongValueStatistics.deserialize(input, available)
                Type.Short -> ShortValueStatistics.deserialize(input, available)
                Type.String -> StringValueStatistics.deserialize(input, available)
                Type.Date -> DateValueStatistics.deserialize(input, available)
                Type.Complex32 -> ValueStatistics(type)
                Type.Complex64 -> ValueStatistics(type)
                is Type.Complex32Vector -> ValueStatistics(type)
                is Type.Complex64Vector -> ValueStatistics(type)
                is Type.BooleanVector -> ValueStatistics(type)
                is Type.DoubleVector -> ValueStatistics(type)
                is Type.FloatVector -> ValueStatistics(type)
                is Type.IntVector -> ValueStatistics(type)
                is Type.LongVector -> ValueStatistics(type)
            }
            stat.dirty = input.readBoolean()
            stat.numberOfNullEntries = input.unpackLong()
            stat.numberOfNonNullEntries = input.unpackLong()
            return stat
        }
    }

    /** Flag indicating that this [ValueStatistics] needs updating. */
    var dirty: Boolean = true
        protected set

    /** Number of null entries for this [ValueStatistics]. */
    var numberOfNullEntries: Long = 0L
        protected set

    /** Number of non-null entries for this [ValueStatistics]. */
    var numberOfNonNullEntries: Long = 0L
        protected set

    /** Total number of entries for this [ValueStatistics]. */
    val numberOfEntries
        get() = this.numberOfNullEntries + this.numberOfNonNullEntries

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
}