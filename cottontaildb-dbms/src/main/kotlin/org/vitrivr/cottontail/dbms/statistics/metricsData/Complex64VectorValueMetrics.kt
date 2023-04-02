package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [Complex64VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Complex64VectorValueMetrics(
    val logicalSize: Int,
    override var numberOfNullEntries: Long = 0L,
    override var numberOfNonNullEntries: Long = 0L,
    override var numberOfDistinctEntries: Long = 0L
): AbstractVectorMetrics<Complex64VectorValue>(Types.Complex64Vector(logicalSize)) {

    /**
     * Constructor for the collector to get from the sample to the population
     */
    constructor(factor: Float, metrics: Complex64VectorValueMetrics): this(
        logicalSize = metrics.logicalSize,
        numberOfNullEntries = (metrics.numberOfNullEntries * factor).toLong(),
        numberOfNonNullEntries = (metrics.numberOfNonNullEntries * factor).toLong(),
        numberOfDistinctEntries = (metrics.numberOfDistinctEntries * factor).toLong(),
    )

    /**
     * Xodus serializer for [Complex64VectorValueMetrics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<Complex64VectorValueMetrics> {
        override fun read(stream: ByteArrayInputStream): Complex64VectorValueMetrics {
            val numberOfNullEntries = LongBinding.readCompressed(stream)
            val numberOfNonNullEntries = LongBinding.readCompressed(stream)
            val numberOfDistinctEntries = LongBinding.readCompressed(stream)

            return Complex64VectorValueMetrics(
                this@Binding.logicalSize,
                numberOfNullEntries,
                numberOfNonNullEntries,
                numberOfDistinctEntries
            )
        }

        override fun write(output: LightOutputStream, statistics: Complex64VectorValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfDistinctEntries)
        }
    }

}