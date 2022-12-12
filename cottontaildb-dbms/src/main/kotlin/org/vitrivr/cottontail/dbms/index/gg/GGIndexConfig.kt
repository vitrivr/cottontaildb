package org.vitrivr.cottontail.dbms.index.gg

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.CosineDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.dbms.index.IndexConfig
import org.vitrivr.cottontail.dbms.index.lsh.LSHIndex
import java.io.ByteArrayInputStream

/**
 * Configuration class for [GGIndex].
 *
 * @author Gabriel Zihlmann
 * @version 1.3.0
 */
data class GGIndexConfig(val distance: Name.FunctionName, val numGroups: Int, val seed: Long) : IndexConfig<GGIndex> {

    companion object {
        /** Configuration key for name of the distance function. */
        const val KEY_DISTANCE_KEY = "distance"

        /** Configuration key for number of groups. */
        const val KEY_NUM_GROUPS_KEY = "num_groups"

        /** Configuration key for the random number generator seed. */
        const val KEY_SEED_KEY = "seed"

        /** The [Name.FunctionName] of the default distance. */
        val DEFAULT_DISTANCE = EuclideanDistance.FUNCTION_NAME

        /** Current list of [Name.FunctionName] supported by the [LSHIndex]. */
        val SUPPORTED_DISTANCES = setOf(ManhattanDistance.FUNCTION_NAME, EuclideanDistance.FUNCTION_NAME, CosineDistance.FUNCTION_NAME)
    }

    /**
     * [ComparableBinding] for [GGIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<GGIndexConfig> = GGIndexConfig(
            Name.FunctionName.create(StringBinding.BINDING.readObject(stream)),
            IntegerBinding.readCompressed(stream),
            LongBinding.readCompressed(stream)
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<GGIndexConfig>) {
            require(`object` is GGIndexConfig) { "GGIndexConfig.Binding can only be used to serialize instances of GGIndexConfig." }
            StringBinding.BINDING.writeObject(output, `object`.distance.simple)
            IntegerBinding.writeCompressed(output, `object`.numGroups)
            LongBinding.writeCompressed(output, `object`.seed)
        }
    }

    init {
        /* Range of sanity checks. */
        require(this.numGroups > 1) { "GGIndex requires at least a single group." }
        require(this.distance in SUPPORTED_DISTANCES) { "GGIndex only support COSINE and INNERPRODUCT distance."}
    }
}