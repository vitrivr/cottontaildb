package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.Complex64Value
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream


/**
 * A [ValueStatistics] implementation for [Complex64Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64ValueStatistics(): AbstractValueStatistics<Complex64Value>(Types.Complex64) {
    /**
     * Xodus serializer for [Complex64ValueStatistics]
     */
    object Binding: XodusBinding<Complex64ValueStatistics> {
        override fun read(stream: ByteArrayInputStream): Complex64ValueStatistics {
            val stat = Complex64ValueStatistics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: Complex64ValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        }
    }

    /**
     * Copies this [Complex64ValueStatistics] and returns it.
     *
     * @return Copy of this [Complex64ValueStatistics].
     */
    override fun copy(): Complex64ValueStatistics {
        val copy = Complex64ValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        return copy
    }
}