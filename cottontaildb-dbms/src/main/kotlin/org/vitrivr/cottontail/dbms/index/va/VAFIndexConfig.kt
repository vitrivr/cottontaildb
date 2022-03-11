package org.vitrivr.cottontail.dbms.index.va

import jetbrains.exodus.bindings.BooleanBinding
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.index.IndexConfig
import org.vitrivr.cottontail.dbms.index.va.signature.VAFMarks
import java.io.ByteArrayInputStream

/**
 * A [IndexConfig] instance for the [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class VAFIndexConfig(val marksPerDimension: Int, val marks: VAFMarks? = null): IndexConfig<VAFIndex> {
    companion object {
        val KEY_MARKS_PER_DIMENSION = "marks_per_dimension"
    }

    /**
     * [ComparableBinding] for [VAFIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<VAFIndexConfig> = VAFIndexConfig(
            IntegerBinding.readCompressed(stream),
            if (BooleanBinding.BINDING.readObject(stream)) {
                VAFMarks.Binding.readObject(stream)
            } else {
                null
            }
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<VAFIndexConfig>) {
            require(`object` is VAFIndexConfig) { "VAFIndexConfig.Binding can only be used to serialize instances of VAFIndexConfig." }
            IntegerBinding.writeCompressed(output, `object`.marksPerDimension)
            if (`object`.marks != null) {
                BooleanBinding.BINDING.writeObject(output, true)
                VAFMarks.Binding.writeObject(output, `object`.marks)
            } else {
                BooleanBinding.BINDING.writeObject(output, false)
            }
        }
    }
}
