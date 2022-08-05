package org.vitrivr.cottontail.dbms.index.va

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.SignedFloatBinding
import jetbrains.exodus.util.LightOutputStream
import java.io.ByteArrayInputStream

/**
 * A statistics entry for the [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class VAFIndexStatistics(val tombstones: Long, val filterEfficiency: Float): Comparable<VAFIndexStatistics> {
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<VAFIndexStatistics> = VAFIndexStatistics(
            LongBinding.readCompressed(stream),
            SignedFloatBinding.BINDING.readObject(stream)
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<VAFIndexStatistics>) {
            require(`object` is VAFIndexStatistics) { "VAFIndexStatistics.Binding can only be used to serialize instances of VAFIndexConfig." }
            LongBinding.writeCompressed(output, `object`.tombstones)
            SignedFloatBinding.BINDING.writeObject(output, `object`.filterEfficiency)
        }
    }

    override fun compareTo(other: VAFIndexStatistics): Int = this.hashCode().compareTo(other.hashCode())
}