package org.vitrivr.cottontail.dbms.statistics.metricsData

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.Complex32Value
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueMetrics] implementation for [Complex32Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex32ValueMetrics(): AbstractScalarMetrics<Complex32Value>(Types.Complex32) {
    /**
     * Xodus serializer for [Complex32ValueMetrics]
     */
    object Binding: MetricsXodusBinding<Complex32ValueMetrics> {
        override fun read(stream: ByteArrayInputStream): Complex32ValueMetrics {
            val stat = Complex32ValueMetrics()
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: Complex32ValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        }
    }
}