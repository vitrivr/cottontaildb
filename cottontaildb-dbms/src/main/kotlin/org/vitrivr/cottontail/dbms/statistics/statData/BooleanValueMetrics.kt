package org.vitrivr.cottontail.dbms.statistics.statData

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [DataMetrics] implementation for [BooleanValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
//data
class BooleanValueMetrics: AbstractValueMetrics<BooleanValue>(Types.Boolean) {
    companion object {
        const val TRUE_ENTRIES_KEY = "true"
        const val FALSE_ENTRIES_KEY = "false"
    }

    /**
     * Xodus serializer for [BooleanValueMetrics]
     */
    //Deactivated for now, since XodusBinding expected [ValueStatistics] which we don't have here
    object Binding: MetricsXodusBinding<BooleanValueMetrics> {
        override fun read(stream: ByteArrayInputStream): BooleanValueMetrics {
            val stat = BooleanValueMetrics()
            // TODO (Number of distinct entries?)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfTrueEntries = LongBinding.readCompressed(stream)
            stat.numberOfFalseEntries = LongBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: BooleanValueMetrics) {
            // TODO (Number of distinct entries?)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfTrueEntries)
            LongBinding.writeCompressed(output, statistics.numberOfFalseEntries)
        }
    }


    /** Number of true entries for in this [BooleanValueMetrics]. */
    var numberOfTrueEntries: Long = 0L
        private set

    /** Number of false entries for in this [BooleanValueMetrics]. */
    var numberOfFalseEntries: Long = 0L
        private set

    /**
     * Resets this [BooleanValueMetrics] and sets all its values to the default value.
     */
    override fun reset() {
        super.reset()
        this.numberOfTrueEntries = 0L
        this.numberOfFalseEntries = 0L
    }

    /**
     * Creates a descriptive map of this [BooleanValueMetrics].
     *
     * @return Descriptive map of this [BooleanValueMetrics]
     */
    override fun about(): Map<String, String> = super.about() + mapOf(
        TRUE_ENTRIES_KEY to this.numberOfTrueEntries.toString(),
        FALSE_ENTRIES_KEY to this.numberOfFalseEntries.toString(),
    )
}