package org.vitrivr.cottontail.dbms.index.lsh

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.CosineDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.InnerProductDistance
import org.vitrivr.cottontail.dbms.index.basic.IndexConfig
import org.vitrivr.cottontail.dbms.index.lsh.signature.LSHSignatureGenerator
import org.vitrivr.cottontail.dbms.index.lsh.signature.SBLSHSignatureGenerator
import java.io.ByteArrayInputStream

/**
 * [IndexConfig] object for [LSHIndex].
 *
 * @author Ralph Gasser, Manuel Hürbin & Gabriel Zihlmann
 * @version 1.0.0
 */
data class LSHIndexConfig(val distance: Name.FunctionName, val buckets: Int, val stages: Int, val seed: Long, val generator: LSHSignatureGenerator? = null): IndexConfig<LSHIndex> {

    companion object {
        /** Configuration key for name of the distance function. */
        const val KEY_DISTANCES = "lsh.distance"

        /** Configuration key for the number of stages. */
        const val KEY_NUM_STAGES = "lsh.stages"

        /** Configuration key for the number of buckets. */
        const val KEY_NUM_BUCKETS = "lsh.buckets"

        /** Configuration key for the random number generator seed. */
        const val KEY_SEED = "lsh.seed"

        /** The [Name.FunctionName] of the default distance. */
        val DEFAULT_DISTANCE = CosineDistance.FUNCTION_NAME

        /** Current list of [Name.FunctionName] supported by the [LSHIndex]. */
        val SUPPORTED_DISTANCES = setOf(CosineDistance.FUNCTION_NAME, InnerProductDistance.FUNCTION_NAME)
    }

    /**
     * [ComparableBinding] for [LSHIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<LSHIndexConfig> = LSHIndexConfig(
            Name.FunctionName.create(StringBinding.BINDING.readObject(stream)),
            IntegerBinding.readCompressed(stream),
            IntegerBinding.readCompressed(stream),
            LongBinding.readCompressed(stream)
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<LSHIndexConfig>) {
            require(`object` is LSHIndexConfig) { "LSHIndexConfig.Binding can only be used to serialize instances of LSHIndexConfig." }
            StringBinding.BINDING.writeObject(output, `object`.distance.simple)
            IntegerBinding.writeCompressed(output, `object`.buckets)
            IntegerBinding.writeCompressed(output, `object`.stages)
            LongBinding.writeCompressed(output, `object`.seed)
        }
    }

    /**
     * Generates and returns a [LSHSignatureGenerator] for this [LSHIndexConfig].
     */
    fun generator(dimension: Int): LSHSignatureGenerator = when(this.distance) {
        CosineDistance.FUNCTION_NAME,
        InnerProductDistance.FUNCTION_NAME -> SBLSHSignatureGenerator(this.stages, this.buckets, this.seed, dimension)
        else -> throw IllegalStateException("The ${this.distance} distance is currently not supported by the LSH index engine.")
    }

    init {
        /* Range of sanity checks. */
        require(this.buckets > 1) { "LSHIndex requires at least two buckets." }
        require(this.stages > 0) { "LSHIndex requires at least a single stage." }
        require(this.distance in SUPPORTED_DISTANCES) { "LSHIndex only support COSINE and INNERPRODUCT distance."}
    }

    /**
     * Converts this [LSHIndexConfig] to a [Map] of key-value pairs.
     *
     * @return [Map]
     */
    override fun toMap(): Map<String, String> = mapOf(
        KEY_DISTANCES to this.distance.simple,
        KEY_NUM_STAGES to this.stages.toString(),
        KEY_NUM_BUCKETS to this.buckets.toString(),
        KEY_SEED to this.seed.toString()
    )
}