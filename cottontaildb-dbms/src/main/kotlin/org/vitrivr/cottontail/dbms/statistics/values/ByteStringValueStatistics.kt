package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.ByteStringValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream

class ByteStringValueStatistics : AbstractValueStatistics<ByteStringValue>(Types.ByteString) {

    object Binding: XodusBinding<ByteStringValueStatistics> {
        override fun read(stream: ByteArrayInputStream): ByteStringValueStatistics {
            val stat = ByteStringValueStatistics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.minWidth = IntegerBinding.readCompressed(stream)
            stat.maxWidth = IntegerBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: ByteStringValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            IntegerBinding.writeCompressed(output, statistics.minWidth)
            IntegerBinding.writeCompressed(output, statistics.maxWidth)
        }
    }


    /** Shortest [StringValue] seen by this [ByteStringValueStatistics] */
    override var minWidth: Int = Int.MAX_VALUE
        private set

    /** Longest [StringValue] seen by this [ByteStringValueStatistics]. */
    override var maxWidth: Int = Int.MIN_VALUE
        private set

    override fun insert(inserted: ByteStringValue?) {
        super.insert(inserted)
        if (inserted != null) {
            this.minWidth = Integer.min(inserted.logicalSize, this.minWidth)
            this.maxWidth = Integer.max(inserted.logicalSize, this.maxWidth)
        }
    }

    override fun delete(deleted: ByteStringValue?) {
        super.delete(deleted)

        /* We cannot create a sensible estimate if a value is deleted. */
        if (this.minWidth == deleted?.logicalSize || this.maxWidth == deleted?.logicalSize) {
            this.fresh = false
        }
    }

    override fun reset() {
        super.reset()
        this.minWidth = Int.MAX_VALUE
        this.maxWidth = Int.MIN_VALUE
    }

    override fun copy(): ValueStatistics<ByteStringValue> {
        val copy = ByteStringValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.minWidth = this.minWidth
        copy.maxWidth = this.maxWidth
        return copy
    }
}