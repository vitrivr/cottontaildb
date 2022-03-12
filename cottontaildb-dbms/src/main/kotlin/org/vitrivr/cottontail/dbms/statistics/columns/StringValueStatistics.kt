package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream
import java.lang.Integer.max
import java.lang.Integer.min
import java.lang.Math.floorDiv

/**
 * A specialized [ValueStatistics] for [StringValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class StringValueStatistics : AbstractValueStatistics<StringValue>(Types.String) {

    /**
     * Xodus serializer for [StringValueStatistics]
     */
    object Binding: XodusBinding<StringValueStatistics> {
        override fun read(stream: ByteArrayInputStream): StringValueStatistics {
            val stat = StringValueStatistics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.minWidth = IntegerBinding.readCompressed(stream)
            stat.maxWidth = IntegerBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: StringValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            IntegerBinding.writeCompressed(output, statistics.minWidth)
            IntegerBinding.writeCompressed(output, statistics.maxWidth)
        }
    }

    /** Shortest [StringValue] seen by this [StringValueStatistics] */
    override var minWidth: Int = Int.MAX_VALUE
        private set

    /** Longest [StringValue] seen by this [StringValueStatistics]. */
    override var maxWidth: Int = Int.MIN_VALUE
        private set

    /** The mean [StringValue] seen by this [StringValueStatistics]. */
    val meanWidth: Int
        get() = floorDiv(this.maxWidth - this.minWidth, 2)

    /**
     * Updates this [StringValueStatistics] with an inserted [StringValue].
     *
     * @param inserted The [StringValue] that was inserted.
     */
    override fun insert(inserted: StringValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.minWidth = min(inserted.logicalSize, this.minWidth)
            this.maxWidth = max(inserted.logicalSize, this.maxWidth)
        }
    }

    /**
     * Updates this [StringValueStatistics] with a new deleted [StringValue].
     *
     * @param deleted The [StringValue] that was deleted.
     */
    override fun delete(deleted: StringValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (this.minWidth == deleted?.logicalSize || this.maxWidth == deleted?.logicalSize) {
            this.fresh = false
        }
    }

    /**
     * Resets this [StringValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.minWidth = Int.MAX_VALUE
        this.maxWidth = Int.MIN_VALUE
    }

    /**
     * Copies this [StringValueStatistics] and returns it.
     *
     * @return Copy of this [StringValueStatistics].
     */
    override fun copy(): StringValueStatistics {
        val copy = StringValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.minWidth = this.minWidth
        copy.maxWidth = this.maxWidth
        return copy
    }
}