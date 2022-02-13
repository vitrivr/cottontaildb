package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.util.LightOutputStream
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.queries.functions.math.VectorDistance
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.index.pq.codebook.DoublePrecisionPQCodebook
import org.vitrivr.cottontail.dbms.index.pq.codebook.PQCodebook
import org.vitrivr.cottontail.dbms.index.pq.codebook.SinglePrecisionPQCodebook
import java.io.ByteArrayInputStream
import java.util.*

/**
 * Product Quantizer (PQ) that minimizes inner product error. Input data should be permuted for better results!
 *
 * Roughly following Guo et al. 2015 - Quantization based Fast Inner Product Search
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 2.0.0
 */
class PQ(name: String, val type: Types<*>, val codebooks: List<PQCodebook<VectorValue<*>>>): IndexStructCatalogueEntry(name) {

    /** The number of subspaces as defined in this [PQ] implementation. */
    val numberOfSubspaces
        get() = this.codebooks.size

    /** The number of dimensions per subspace which is simply the dimensionality of the [ColumnDef] divided by the number of subspaces. */
    val dimensionsPerSubspace
        get() = this.type.logicalSize / this.codebooks.size

    /**
     *
     */
    companion object: ComparableBinding() {
        /** [Logger] instance used for logging.. */
        private val LOGGER: Logger = LoggerFactory.getLogger(PQ::class.java)

        /** Maximum number of iterations to use for k means clustering. */
        private const val MAX_ITERATIONS = 250

        /**
         * Generates a new [PQ] instance for the given [PQIndexConfig]
         */
        @Suppress("UNCHECKED_CAST")
        fun fromData(index: PQIndex, data: List<VectorValue<*>>): PQ {
            LOGGER.debug("Initializing PQ from initial data.")

            /* Sanity checks. */
            require(index.config.numSubspaces > 0) { "Number of subspaces must be greater than zero for PQIndex." }
            require(index.config.numCentroids > 0) { "Number of centroids must be greater than zero for PQIndex." }
            require(index.columns[0].type.logicalSize >= index.config.numSubspaces) { "Logical size of column must be greater or equal to number of subspaces." }

            /* Calculate some important metrics. */
            val dimensionsPerSubspace = index.columns[0].type.logicalSize / index.config.numSubspaces

            /* Prepare subspace data. */
            LOGGER.debug("Creating subspace data")
            val subspaceData = (0 until index.config.numSubspaces).map { k ->
                data.map { v -> v.subvector(k * dimensionsPerSubspace, dimensionsPerSubspace) }
            }

            /* Start learning of centroids. */
            LOGGER.debug("Learning centroids")
            val codebooks = mutableListOf<PQCodebook<VectorValue<*>>>()
            subspaceData.parallelStream().forEach { d ->
                val codebook: PQCodebook<*> = when (index.columns[0].type) {
                    is Types.Complex32Vector,
                    is Types.FloatVector -> SinglePrecisionPQCodebook.learnFromData(
                        d as List<FloatVectorValue>,
                        index.config.numCentroids,
                        index.config.seed,
                        MAX_ITERATIONS
                    )
                    is Types.Complex64Vector,
                    is Types.DoubleVector -> DoublePrecisionPQCodebook.learnFromData(
                        d as List<DoubleVectorValue>,
                        index.config.numCentroids,
                        index.config.seed,
                        MAX_ITERATIONS
                    )
                    else -> throw IllegalArgumentException("VectorValue of type ${index.columns[0].type} is not supported for PQ.")
                }
                codebooks.add(codebook as PQCodebook<VectorValue<*>>)
            }

            LOGGER.debug("PQ initialization done.")
            return PQ(index.name.toString() + "_pq", index.columns[0].type, codebooks)
        }

        override fun readObject(stream: ByteArrayInputStream): Comparable<Nothing> {
            TODO("Not yet implemented")
        }

        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            TODO("Not yet implemented")
        }
    }

    /**
     * Builds and returns the [PQSignature] for the specified [VectorValue],  which is simply a
     * concatenation of the representative centroid in each subspace for the specified vector
     *
     * @param v The [VectorValue] to calculate the [PQSignature] for.
     * @return The calculated [PQSignature]
     */
    fun getSignature(v: VectorValue<*>): PQSignature =
        getSignature(v, IntArray(this.numberOfSubspaces))

    /**
     * Builds and returns the [PQSignature] for the specified [VectorValue],  which is simply a
     * concatenation of the representative centroid in each subspace for the specified vector
     *
     * @param v The [VectorValue] to calculate the [PQSignature] for.
     * @return The calculated [PQSignature]
     */
    fun getSignature(v: VectorValue<*>, array: IntArray): PQSignature {
        require(array.size == this.numberOfSubspaces) { "IntArray of signature must have the same size as the number of subspaces for this PQ (expected: ${this.numberOfSubspaces}, actual: ${array.size})." }
        Arrays.parallelSetAll(array) { k ->
            this.codebooks[k].quantizeSubspaceForVector(v, k * this.dimensionsPerSubspace)
        }
        return PQSignature(array)
    }

    /**
     * Generates and returns a [PQLookupTable] for the given [VectorDistance].
     *
     * @param distance The [VectorDistance] to generate the [PQLookupTable] for.
     * @return The [PQLookupTable] for the given [VectorDistance].
     */
    fun getLookupTable(query: VectorValue<*>, distance: VectorDistance<*>): PQLookupTable {
        val reshape = distance.copy(this.dimensionsPerSubspace)
        return PQLookupTable(
            Array(this.numberOfSubspaces) { k ->
                val codebook = this.codebooks[k]
                val subspaceQuery = query.subvector(k * this.dimensionsPerSubspace, this.dimensionsPerSubspace)
                DoubleArray(codebook.numberOfCentroids) { reshape(subspaceQuery, codebook[it])!!.value }
            }
        )
    }
}
