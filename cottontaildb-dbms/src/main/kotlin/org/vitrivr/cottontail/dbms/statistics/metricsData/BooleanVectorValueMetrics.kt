package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [BooleanVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
data class BooleanVectorValueMetrics(val logicalSize: Int) : AbstractVectorMetrics<BooleanVectorValue>(Types.BooleanVector(logicalSize)) {

    constructor(
        logicalSize: Int,
        numberOfTrueEntries: LongArray,
        numberOfNullEntries: Long,
        numberOfNonNullEntries: Long,
        numberOfDistinctEntries: Long
    ) : this(logicalSize) {
        this.numberOfTrueEntries = numberOfTrueEntries
        this.numberOfNullEntries = numberOfNullEntries
        this.numberOfNonNullEntries = numberOfNonNullEntries
        this.numberOfDistinctEntries = numberOfDistinctEntries
    }

    /**
     * Xodus serializer for [BooleanVectorValueMetrics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<BooleanVectorValueMetrics> {
        override fun read(stream: ByteArrayInputStream): BooleanVectorValueMetrics {
            val stat = BooleanVectorValueMetrics(this.logicalSize)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            for (i in 0 until this.logicalSize) {
                stat.numberOfTrueEntries[i] = LongBinding.readCompressed(stream)
                stat.numberOfFalseEntries[i] = LongBinding.readCompressed(stream)
            }
            return stat
        }

        override fun write(output: LightOutputStream, statistics: BooleanVectorValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            for (i in 0 until statistics.type.logicalSize) {
                LongBinding.writeCompressed(output, statistics.numberOfTrueEntries[i])
                LongBinding.writeCompressed(output, statistics.numberOfFalseEntries[i])
            }
        }
    }

    /** A histogram capturing the number of true entries per component. */
    var numberOfTrueEntries: LongArray = LongArray(this.type.logicalSize)

    /** A histogram capturing the number of false entries per component. */
    val numberOfFalseEntries: LongArray
        get() = LongArray(this.type.logicalSize) {
            this.numberOfNonNullEntries - this.numberOfTrueEntries[it]
        }

    /**
     * Resets this [BooleanVectorValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        for (i in 0 until this.type.logicalSize) {
            this.numberOfTrueEntries[i] = 0L
        }
    }
}