package org.vitrivr.cottontail.dbms.statistics.statData

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream

/**
 * A [DataMetrics] implementation for [Complex32VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex32VectorValueMetrics(logicalSize: Int): AbstractValueMetrics<Complex32VectorValue>(Types.Complex32Vector(logicalSize)) {
    /**
     * Xodus serializer for [Complex32VectorValueMetrics]
     */
    class Binding(val logicalSize: Int): MetricsXodusBinding<Complex32VectorValueMetrics> {
        override fun read(stream: ByteArrayInputStream): Complex32VectorValueMetrics {
            val stat = Complex32VectorValueMetrics(this.logicalSize)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: Complex32VectorValueMetrics) {
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        }
    }

}