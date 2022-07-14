package org.vitrivr.cottontail.dbms.index.va

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.index.basic.IndexConfig
import java.io.ByteArrayInputStream

/**
 * A [IndexConfig] instance for the [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class VAFIndexConfig(val marksPerDimension: Int): IndexConfig<VAFIndex> {
    companion object {
        const val KEY_MARKS_PER_DIMENSION = "vaf.marks.per.dimension"
    }

    /**
     * [ComparableBinding] for [VAFIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<VAFIndexConfig> = VAFIndexConfig(
            IntegerBinding.readCompressed(stream),
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<VAFIndexConfig>) {
            require(`object` is VAFIndexConfig) { "VAFIndexConfig.Binding can only be used to serialize instances of VAFIndexConfig." }
            IntegerBinding.writeCompressed(output, `object`.marksPerDimension)
        }
    }
}
