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
class Complex64VectorValueMetrics(logicalSize: Int): AbstractVectorMetrics<Complex64VectorValue>(Types.Complex64Vector(logicalSize)) {
    /**
     * Xodus serializer for [Complex64VectorValueMetrics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<Complex64VectorValueMetrics> {
        override fun read(stream: ByteArrayInputStream): Complex64VectorValueMetrics {
            val stat = Complex64VectorValueMetrics(this.logicalSize)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: Complex64VectorValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        }
    }

}