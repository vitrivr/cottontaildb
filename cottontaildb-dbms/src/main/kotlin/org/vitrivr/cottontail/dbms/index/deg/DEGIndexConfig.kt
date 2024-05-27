package org.vitrivr.cottontail.dbms.index.deg

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.*
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.euclidean.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.squaredeuclidean.SquaredEuclideanDistance
import org.vitrivr.cottontail.dbms.index.basic.IndexConfig
import org.vitrivr.cottontail.dbms.index.pq.PQIndexConfig
import java.io.ByteArrayInputStream


data class DEGIndexConfig(val distance: Name.FunctionName, val degree: Int, val epsilonExt: Float = DEFAULT_EPSILON_EXT, val kExt: Int = degree * 2): IndexConfig<DEGIndex> {
    companion object {
        /** Configuration key for distance function used to train the DEG. */
        const val KEY_DISTANCE = "deg.distance"

        /** Configuration key for the degree of the DEG. */
        const val KEY_DEGREE = "deg.degree"

        /** Configuration key for the epsilon value upon extension. */
        const val KEY_EPSILON_EXT = "deg.epsExt"

        /** Configuration key for the k value upon extension. */
        const val KEY_K_EXT = "pq.kExt"

        /** Default value for the degree. */
        const val DEFAULT_DEGREE = 16

        /** Default value for the epsilon value upon extension. */
        const val DEFAULT_EPSILON_EXT = 0.2f

        /** Set of supported distances. */
        val SUPPORTED_DISTANCES: Set<Name.FunctionName> = setOf(
            ManhattanDistance.FUNCTION_NAME,
            EuclideanDistance.FUNCTION_NAME,
            SquaredEuclideanDistance.FUNCTION_NAME,
            CosineDistance.FUNCTION_NAME,
            InnerProductDistance.FUNCTION_NAME
        )
    }

    init {
        require(this.degree >= 4) { "The degree of a Dynamic Exploration Graph (DEG) must be greater than four." }
        require(this.degree % 2 == 0) { "Dynamic Exploration Graph (DEG) must be even-regular." }
        require(this.distance in SUPPORTED_DISTANCES) { "The distance function $distance is not supported by the Dynamic Exploration Graph (DEG)." }
    }


    /**
     * [ComparableBinding] for [DEGIndexConfig].
     */
    object Binding: ComparableBinding() {
        override fun readObject(stream: ByteArrayInputStream): Comparable<Nothing> = DEGIndexConfig(
            Name.FunctionName.create(StringBinding.BINDING.readObject(stream)),
            IntegerBinding.readCompressed(stream),
            FloatBinding.BINDING.readObject(stream),
            IntegerBinding.readCompressed(stream)
        )

        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is DEGIndexConfig) { "DEGIndexConfig.Binding can only be used to serialize instances of PQIndexConfig." }
            StringBinding.BINDING.writeObject(output, `object`.distance.simple)
            IntegerBinding.writeCompressed(output, `object`.degree)
            FloatBinding.BINDING.writeObject(output, `object`.epsilonExt)
            IntegerBinding.writeCompressed(output, `object`.kExt)
        }

    }

    /**
     * Converts this [PQIndexConfig] to a [Map] of key-value pairs.
     *
     * @return [Map]
     */
    override fun toMap(): Map<String, String> = mapOf(
        KEY_DEGREE to this.degree.toString(),
        KEY_EPSILON_EXT to this.epsilonExt.toString(),
        KEY_K_EXT to this.kExt.toString(),
    )
}