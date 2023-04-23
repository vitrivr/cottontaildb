package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A specialized [ValueMetrics] implementation for [StringValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
data class StringValueMetrics(
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L,
    override var minWidth: Int = Int.MAX_VALUE,
    override var maxWidth: Int = Int.MIN_VALUE
) : AbstractScalarMetrics<StringValue>(Types.String) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: StringValueMetrics): this(
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = (metrics.numberOfDistinctEntries * factor).toLong(),
        minWidth = metrics.minWidth,
        maxWidth = metrics.maxWidth,
    )

    /**
     * Xodus serializer for [StringValueMetrics]
     */
    object Binding: MetricsXodusBinding<StringValueMetrics> {
        override fun read(stream: ByteArrayInputStream): StringValueMetrics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)
            val minWidth = IntegerBinding.readCompressed(stream)
            val maxWidth = IntegerBinding.readCompressed(stream)
            return StringValueMetrics(numberOfNullEntries, numberOfNonNullEntries, numberOfDistinctEntries, minWidth, maxWidth)
        }

        override fun write(output: LightOutputStream, statistics: StringValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
            IntegerBinding.writeCompressed(output, statistics.minWidth)
            IntegerBinding.writeCompressed(output, statistics.maxWidth)
        }
    }

    /**
     * Resets this [StringValueMetrics] and sets all its values to to the default value.
     */
    override fun reset() {
        super.reset()
        this.minWidth = Int.MAX_VALUE
        this.maxWidth = Int.MIN_VALUE
    }

    /**
     * Creates a descriptive map of this [ValueMetrics].
     *
     * @return Descriptive map of this [ValueMetrics]
     */
    override fun about(): Map<String, String> {
        return super.about() + mapOf(
            MIN_WIDTH_KEY to this.minWidth.toString(),
            MAX_WIDTH_KEY to this.maxWidth.toString()
        )
    }

}