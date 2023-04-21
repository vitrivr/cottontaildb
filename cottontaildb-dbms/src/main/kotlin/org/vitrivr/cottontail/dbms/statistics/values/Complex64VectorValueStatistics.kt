package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [Complex64VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex64VectorValueStatistics(logicalSize: Int): AbstractValueStatistics<Complex64VectorValue>(Types.Complex64Vector(logicalSize)) {
    /**
     * Xodus serializer for [Complex64VectorValueStatistics]
     */
    class Binding(val logicalSize: Int): XodusBinding<Complex64VectorValueStatistics> {
        override fun read(stream: ByteArrayInputStream): Complex64VectorValueStatistics {
            val stat = Complex64VectorValueStatistics(this.logicalSize)
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: Complex64VectorValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        }
    }

    /**
     * Copies this [Complex64VectorValueStatistics] and returns it.
     *
     * @return Copy of this [Complex64VectorValueStatistics].
     */
    override fun copy(): Complex64VectorValueStatistics {
        val copy = Complex64VectorValueStatistics(this.type.logicalSize)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        return copy
    }
}