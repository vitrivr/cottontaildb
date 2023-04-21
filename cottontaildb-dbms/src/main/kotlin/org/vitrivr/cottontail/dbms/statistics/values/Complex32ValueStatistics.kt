package org.vitrivr.cottontail.dbms.statistics.values

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.values.Complex32Value
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.storage.serializers.statistics.xodus.XodusBinding
import java.io.ByteArrayInputStream

/**
 * A [ValueStatistics] implementation for [Complex32Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Complex32ValueStatistics(): AbstractValueStatistics<Complex32Value>(Types.Complex32) {
    /**
     * Xodus serializer for [Complex32ValueStatistics]
     */
    object Binding: XodusBinding<Complex32ValueStatistics> {
        override fun read(stream: ByteArrayInputStream): Complex32ValueStatistics {
            val stat = Complex32ValueStatistics()
            stat.fresh = BooleanBinding.BINDING.readObject(stream)
            stat.numberOfNullEntries = LongBinding.readCompressed(stream)
            stat.numberOfNonNullEntries = LongBinding.readCompressed(stream)
            return stat
        }

        override fun write(output: LightOutputStream, statistics: Complex32ValueStatistics) {
            BooleanBinding.BINDING.writeObject(output, statistics.fresh)
            LongBinding.writeCompressed(output, statistics.numberOfNullEntries)
            LongBinding.writeCompressed(output, statistics.numberOfNonNullEntries)
        }
    }

    /**
     * Copies this [BooleanValueStatistics] and returns it.
     *
     * @return Copy of this [BooleanValueStatistics].
     */
    override fun copy(): Complex32ValueStatistics {
        val copy = Complex32ValueStatistics()
        copy.fresh = this.fresh
        copy.numberOfNullEntries = this.numberOfNullEntries
        copy.numberOfNonNullEntries = this.numberOfNonNullEntries
        return copy
    }
}