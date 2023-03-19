package org.vitrivr.cottontail.dbms.statistics.statData

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.Complex64Value
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.MetricsXodusBinding
import java.io.ByteArrayInputStream


/**
 * A [DataMetrics] implementation for [Complex64Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64ValueMetrics(): AbstractValueMetrics<Complex64Value>(Types.Complex64) {
    /**
     * Xodus serializer for [Complex64ValueMetrics]
     */
    object Binding: MetricsXodusBinding<Complex64ValueMetrics> {
        override fun read(stream: ByteArrayInputStream): Complex64ValueMetrics {
            val stat = Complex64ValueMetrics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: Complex64ValueMetrics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        }
    }

}