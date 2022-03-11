package org.vitrivr.cottontail.dbms.index.lsh

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.functions.math.distance.Distances
import org.vitrivr.cottontail.dbms.index.IndexConfig
import org.vitrivr.cottontail.dbms.index.lsh.signature.LSHSignatureGenerator
import org.vitrivr.cottontail.dbms.index.lsh.signature.SBLSHSignatureGenerator
import java.io.ByteArrayInputStream

/**
 * [IndexConfig] object for [LSHIndex].
 *
 * @author Ralph Gasser, Manuel Hürbin & Gabriel Zihlmann
 * @version 1.0.0
 */
data class LSHIndexConfig(val buckets: Int, val stages: Int, val seed: Long, val distance: Distances, val generator: LSHSignatureGenerator? = null): IndexConfig<LSHIndex> {

    companion object {
        const val KEY_SEED = "seed"
        const val KEY_NUM_STAGES = "stages"
        const val KEY_NUM_BUCKETS = "buckets"
        const val KEY_DISTANCES = "distances"

        /** Current list of [Distances] supported by the [LSHIndex]. */
        val SUPPORTED_DISTANCES = setOf(Distances.COSINE.functionName, Distances.INNERPRODUCT.functionName)
    }

    /**
     * [ComparableBinding] for [LSHIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<LSHIndexConfig> = LSHIndexConfig(
            IntegerBinding.readCompressed(stream),
            IntegerBinding.readCompressed(stream),
            LongBinding.readCompressed(stream),
            Distances.values()[IntegerBinding.readCompressed(stream)]
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<LSHIndexConfig>) {
            require(`object` is LSHIndexConfig) { "LSHIndexConfig.Binding can only be used to serialize instances of LSHIndexConfig." }
            IntegerBinding.writeCompressed(output, `object`.buckets)
            IntegerBinding.writeCompressed(output, `object`.stages)
            LongBinding.writeCompressed(output, `object`.seed)
            IntegerBinding.writeCompressed(output, `object`.distance.ordinal)
        }
    }

    /**
     * Generates and returns a [LSHSignatureGenerator] for this [LSHIndexConfig].
     */
    fun generator(dimension: Int): LSHSignatureGenerator = when(this.distance) {
        Distances.COSINE, Distances.INNERPRODUCT -> SBLSHSignatureGenerator(this.stages, this.buckets, this.seed, dimension)
        else -> throw IllegalStateException("The ${this.distance} distance is currently not supported by the LSH index engine.")
    }
}