package org.vitrivr.cottontail.database.statistics.columns

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A specialized [ValueStatistics] for [StringValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class StringValueStatistics : ValueStatistics<StringValue>(Type.String) {

    /**
     * Serializer for [StringValueStatistics].
     */
    companion object Serializer : org.mapdb.Serializer<StringValueStatistics> {
        override fun serialize(out: DataOutput2, value: StringValueStatistics) {
            out.packInt(value.minLength)
            out.packInt(value.maxLength)
        }

        override fun deserialize(input: DataInput2, available: Int): StringValueStatistics {
            val stat = StringValueStatistics()
            stat.minLength = input.unpackInt()
            stat.maxLength = input.unpackInt()
            return stat
        }
    }

    /** Smallest [StringValue] seen by this [ValueStatistics] */
    var minLength: Int = Int.MAX_VALUE

    /** Number of null entries contained in this [ValueStatistics]. */
    var maxLength: Int = Int.MIN_VALUE

    /**
     * Updates this [StringValueStatistics] with an inserted [StringValue].
     *
     * @param inserted The [Value] that was deleted.
     */
    override fun insert(inserted: StringValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.minLength = min(inserted.logicalSize, this.minLength)
            this.maxLength = max(inserted.logicalSize, this.maxLength)
        }
    }

    /**
     * Updates this [StringValueStatistics] with a new deleted [StringValue].
     *
     * @param deleted The [Value] that was deleted.
     */
    override fun delete(deleted: StringValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (deleted != null) {
            if (this.minLength == deleted.logicalSize || this.maxLength == deleted.logicalSize) {
                this.dirty = true
            }
        }
    }
}