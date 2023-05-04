package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [BooleanValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class BooleanValueStatistics: AbstractValueStatistics<BooleanValue>(Types.Boolean) {
    companion object {
        const val TRUE_ENTRIES_KEY = "true"
        const val FALSE_ENTRIES_KEY = "false"
    }

    /**
     * Xodus serializer for [BooleanValueStatistics]
     */
    object Binding: XodusBinding<BooleanValueStatistics> {
        override fun read(stream: ByteArrayInputStream): BooleanValueStatistics {
            val stat = BooleanValueStatistics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfTrueEntries = LongBinding.readCompressed(stream)
            stat.numberOfFalseEntries = LongBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: BooleanValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfTrueEntries)
            LongBinding.writeCompressed(output, statistics.numberOfFalseEntries)
        }
    }

    /** Number of true entries for in this [BooleanValueStatistics]. */
    var numberOfTrueEntries: Long = 0L
        private set

    /** Number of false entries for in this [BooleanValueStatistics]. */
    var numberOfFalseEntries: Long = 0L
        private set

    /**
     * Updates this [BooleanValueStatistics] with an inserted [BooleanValue]
     *
     * @param inserted The [BooleanValue] that was inserted.
     */
    override fun insert(inserted: BooleanValue?) {
        when (inserted?.value) {
            null -> this.numberOfNullEntries += 1
            true -> {
                this.numberOfTrueEntries += 1
                this.numberOfNonNullEntries += 1
            }
            false -> {
                this.numberOfFalseEntries += 1
                this.numberOfNonNullEntries += 1
            }
        }
    }

    /**
     * Updates this [LongValueStatistics] with a deleted [BooleanValue]
     *
     * @param deleted The [BooleanValue] that was deleted.
     */
    override fun delete(deleted: BooleanValue?) {
        when (deleted?.value) {
            null -> this.numberOfNullEntries -= 1
            true -> {
                this.numberOfTrueEntries -= 1
                this.numberOfNonNullEntries -= 1
            }
            false -> {
                this.numberOfFalseEntries -= 1
                this.numberOfNonNullEntries -= 1
            }
        }
    }

    /**
     * Resets this [BooleanValueStatistics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.numberOfTrueEntries = 0L
        this.numberOfFalseEntries = 0L
    }

    /**
     * Creates a descriptive map of this [BooleanValueStatistics].
     *
     * @return Descriptive map of this [BooleanValueStatistics]
     */
    override fun about(): Map<String, String> = super.about() + mapOf(
        TRUE_ENTRIES_KEY to this.numberOfTrueEntries.toString(),
        FALSE_ENTRIES_KEY to this.numberOfFalseEntries.toString(),
    )

    /**
     * Copies this [BooleanValueStatistics] and returns it.
     *
     * @return Copy of this [BooleanValueStatistics].
     */
    override fun copy(): BooleanValueStatistics {
        val copy = BooleanValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        copy.numberOfTrueEntries = this.numberOfTrueEntries
        copy.numberOfTrueEntries = this.numberOfTrueEntries
        return copy
    }
}
