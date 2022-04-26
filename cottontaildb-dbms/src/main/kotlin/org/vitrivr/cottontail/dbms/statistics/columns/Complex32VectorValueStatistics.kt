package org.vitrivr.cottontail.dbms.statistics.columns

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [Complex32VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex32VectorValueStatistics(logicalSize: Int): AbstractValueStatistics<Complex32VectorValue>(Types.Complex32Vector(logicalSize)) {
    /**
     * Xodus serializer for [Complex32VectorValueStatistics]
     */
    class Binding(val logicalSize: Int): XodusBinding<Complex32VectorValueStatistics> {
        override fun read(stream: ByteArrayInputStream): Complex32VectorValueStatistics {
            val stat = Complex32VectorValueStatistics(this.logicalSize)
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: Complex32VectorValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        }
    }

    /**
     * Copies this [Complex32VectorValueStatistics] and returns it.
     *
     * @return Copy of this [Complex32VectorValueStatistics].
     */
    override fun copy(): Complex32VectorValueStatistics {
        val copy = Complex32VectorValueStatistics(this.type.logicalSize)
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        return copy
    }
}