package org.vitrivr.cottontail.database.index.pq

import org.apache.commons.math3.stat.correlation.Covariance
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.database.serializers.FixedComplex32VectorSerializer
import org.vitrivr.cottontail.database.serializers.FixedComplex64VectorSerializer
import org.vitrivr.cottontail.database.serializers.FixedDoubleVectorSerializer
import org.vitrivr.cottontail.database.serializers.FixedFloatVectorSerializer
import org.vitrivr.cottontail.model.values.Complex32VectorValue
import org.vitrivr.cottontail.model.values.Complex64VectorValue
import org.vitrivr.cottontail.model.values.DoubleVectorValue
import org.vitrivr.cottontail.model.values.FloatVectorValue
import org.vitrivr.cottontail.model.values.types.ComplexVectorValue
import org.vitrivr.cottontail.model.values.types.NumericValue
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * Product Quantizer that minimizes inner product error. input data should be permuted for better results!
 * author: Gabriel Zihlmann
 * date: 25.8.2020
 * Roughly following Guo et al. 2015 - Quantization based Fast Inner Product Search
 */
class PQ(val codebooks: Array<PQCodebook<out VectorValue<*>>>, val type: ColumnType<out VectorValue<*>>) {
    companion object Serializer : org.mapdb.Serializer<PQ> {
        private val LOGGER: Logger = LoggerFactory.getLogger(PQ::class.java)
        const val MAX_ITERATIONS = 250
        fun fromPermutedData(numSubspaces: Int, numCentroids: Int, permutedData: Array<out VectorValue<*>>, type: ColumnType<out VectorValue<*>>, seed: Long): Pair<PQ, Array<IntArray>> {
            LOGGER.debug("Initializing PQ from initial data.")
            // some assumptions. Some are for documentation, some are cheap enough to actually keep and check
            require(permutedData.all { it.logicalSize == permutedData[0].logicalSize && it::class.java == permutedData[0]::class.java })
            require(permutedData[0]::class == type.type) { "Data and type arguments must agree!" }
            require(numSubspaces > 0)
            require(numCentroids > 0)
            require(permutedData[0].logicalSize >= numSubspaces)
            require(permutedData[0].logicalSize % numSubspaces == 0)

            val dimensionsPerSubspace = permutedData[0].logicalSize / numSubspaces
            val subspaceSignatures = Array(permutedData.size) { IntArray(numSubspaces) }
            LOGGER.debug("Creating subspace data")
            // wasteful copy...
            val permutedSubspaceData = (0 until numSubspaces).map { k ->
                k to permutedData.map { v -> v.get(k * dimensionsPerSubspace, dimensionsPerSubspace) }.toTypedArray()
            }
            LOGGER.debug("Learning centroids")
            val codebooks = Array<PQCodebook<out VectorValue<*>>?>(numSubspaces) { null }
            permutedSubspaceData.parallelStream().forEach { (k, permutedData) ->
                LOGGER.trace("Processing subspace ${k + 1}")
                val (codebook: PQCodebook<out VectorValue<*>>, signatures) = when (permutedData[0]) {
                    is RealVectorValue<*> -> {
                        PQCodebook.learnFromRealData(permutedData.map { it as RealVectorValue<*> }.toTypedArray(), numCentroids, MAX_ITERATIONS, seed)
                    }
                    is ComplexVectorValue<*> -> {
                        PQCodebook.learnFromComplexData(permutedData.map { it as ComplexVectorValue<*> }.toTypedArray(), numCentroids, MAX_ITERATIONS, seed)
                    }
                    else -> {
                        error("Unknown type")
                    }
                }
                signatures.forEachIndexed { i, c ->
                    subspaceSignatures[i][k] = c
                }
                codebooks[k] = codebook
                LOGGER.trace("Done processing subspace ${k + 1} of $numSubspaces")
            }
            LOGGER.debug("PQ initialization done.")
            return PQ(codebooks.map { it!! }.toTypedArray(), type) to subspaceSignatures
        }

        fun fromPermutedData(numSubspaces: Int, numCentroids: Int, permutedData: Array<DoubleArray>, permutedExampleQueryData: Array<DoubleArray>? = null, seed: Long): Pair<PQ, Array<IntArray>> {
            LOGGER.debug("Initializing PQ from initial data.")
            // some assumptions. Some are for documentation, some are cheap enough to actually keep and check
            require(permutedData.all { it.size == permutedData[0].size })
            require(numSubspaces > 0)
            require(numCentroids > 0)
            require(permutedData[0].size >= numSubspaces)
            require(permutedData[0].size % numSubspaces == 0)
            val dimensionsPerSubspace = permutedData[0].size / numSubspaces
            val subspaceSignatures = Array(permutedData.size) { IntArray(numSubspaces) }
            LOGGER.debug("Creating subspace data")
            // wasteful copy...
            val permutedSubspaceData = splitDataIntoSubspaces(numSubspaces, permutedData, dimensionsPerSubspace).mapIndexed { k, ssData -> k to ssData }
            val permutedSubspaceExampleData = permutedExampleQueryData?.let { splitDataIntoSubspaces(numSubspaces, it, dimensionsPerSubspace) }
            LOGGER.debug("Learning centroids")
            val codebooks = Array<PQCodebook<DoubleVectorValue>?>(numSubspaces) { null }
            permutedSubspaceData.parallelStream().forEach { (k, permutedData) ->
                LOGGER.trace("Processing subspace ${k + 1}")
                val (codebook, signatures) = if (permutedSubspaceExampleData != null) {
                    val qCovMatrix = Covariance(permutedSubspaceExampleData[k], false).covarianceMatrix
                    PQCodebook.learnFromRealData(permutedData, qCovMatrix, numCentroids, MAX_ITERATIONS, seed)
                } else {
                    PQCodebook.learnFromRealData(permutedData, numCentroids, MAX_ITERATIONS, seed)
                }
                signatures.forEachIndexed { i, c ->
                    subspaceSignatures[i][k] = c
                }
                codebooks[k] = codebook
                LOGGER.debug("Done processing subspace ${k + 1} of $numSubspaces")
            }
            LOGGER.debug("PQ initialization done.")
            return PQ(codebooks.map { it!! }.toTypedArray(), DoubleVectorColumnType) to subspaceSignatures
        }

        private fun splitDataIntoSubspaces(numSubspaces: Int, permutedData: Array<DoubleArray>, dimensionsPerSubspace: Int): List<Array<DoubleArray>> {
            return (0 until numSubspaces).map { k ->
                Array(permutedData.size) { i ->
                    DoubleArray(dimensionsPerSubspace) { j ->
                        permutedData[i][k * dimensionsPerSubspace + j]
                    }
                }
            }
        }

        /**
         * Serializes the content of the given value into the given
         * [DataOutput2].
         * todo: figure out why cleaner way via value.type.serializer(size) does not work... it's a generics issue
         *       wrt. in/out
         *
         * @param out DataOutput2 to save object into
         * @param value Object to serialize
         *
         * @throws IOException in case of an I/O error
         */
        override fun serialize(out: DataOutput2, value: PQ) {
            out.packInt(value.numSubspaces)
            out.packInt(value.numCentroids)
            out.packInt(value.dimensionsPerSubspace)
            out.writeUTF(value.type.name)
            value.codebooks.forEach {
                it.centroids.forEach { c ->
                    when (value.type) {
                        is Complex32VectorColumnType -> value.type.serializer(value.dimensionsPerSubspace).serialize(out, c as Complex32VectorValue)
                        is Complex64VectorColumnType -> value.type.serializer(value.dimensionsPerSubspace).serialize(out, c as Complex64VectorValue)
                        is DoubleVectorColumnType -> value.type.serializer(value.dimensionsPerSubspace).serialize(out, c as DoubleVectorValue)
                        is FloatVectorColumnType -> value.type.serializer(value.dimensionsPerSubspace).serialize(out, c as FloatVectorValue)
                        else -> TODO("Other types not yet supported")
                    }
                }
                val cov = it.dataCovarianceMatrix //first dim are rows
                for (i in 0 until value.dimensionsPerSubspace) {
                    when (value.type) {
                        is Complex32VectorColumnType -> FixedComplex32VectorSerializer(value.dimensionsPerSubspace).serialize(out, cov[i] as Complex32VectorValue)
                        is Complex64VectorColumnType -> FixedComplex64VectorSerializer(value.dimensionsPerSubspace).serialize(out, cov[i] as Complex64VectorValue)
                        is DoubleVectorColumnType -> FixedDoubleVectorSerializer(value.dimensionsPerSubspace).serialize(out, cov[i] as DoubleVectorValue)
                        is FloatVectorColumnType -> FixedFloatVectorSerializer(value.dimensionsPerSubspace).serialize(out, cov[i] as FloatVectorValue)
                        else -> TODO("Other types not yet supported")
                    }
                }
            }
        }

        /**
         * Deserializes and returns the content of the given [DataInput2].
         *
         * @param input DataInput2 to de-serialize data from
         * @param available how many bytes that are available in the DataInput2 for
         * reading, may be -1 (in streams) or 0 (null).
         *
         * @return the de-serialized content of the given [DataInput2]
         * @throws IOException in case of an I/O error
         */
        override fun deserialize(input: DataInput2, available: Int): PQ {
            val numSubspaces = input.unpackInt()
            val numCentroids = input.unpackInt()
            val dimensionsPerSubspace = input.unpackInt()
            @Suppress("UNCHECKED_CAST")
            val type = ColumnType.forName(input.readUTF()) as ColumnType<out VectorValue<*>>
            val s = type.serializer(dimensionsPerSubspace)
            return PQ(Array(numSubspaces) {
                PQCodebook(Array(numCentroids) {
                    type.cast(s.deserialize(input, available - input.pos))!!
                },
                Array(dimensionsPerSubspace) {
                    type.cast(s.deserialize(input, available - input.pos))!!
                })
            },
            type)
        }
    }

    val numSubspaces = codebooks.size
    val numCentroids: Int
    val dimensionsPerSubspace: Int

    init {
        require(codebooks.all {
            it.centroids.size == codebooks[0].centroids.size
        })
        dimensionsPerSubspace = codebooks[0].centroids[0].logicalSize
        numCentroids = codebooks[0].centroids.size
    }


    /**
     * Calculates the IP between
     * the approximation specified with the index and the supplied vector which was permuted with the same permutation
     * that was applied to the data when creating this [PQ] object.
     * todo: this always copies! baaad
     */
    fun approximateAsymmetricIP(sigi: IntArray, v: DoubleArray): Double {
//        require(v.size == numSubspaces * dimensionsPerSubspace)
        var res = codebooks[0].centroids[sigi[0]].dot(DoubleVectorValue(v.slice(0 until dimensionsPerSubspace)))
        for (k in 1 until numSubspaces) {
            val centi = codebooks[k].centroids[sigi[k]]
            res += centi.dot(DoubleVectorValue(v.slice(k * dimensionsPerSubspace until (k + 1) * dimensionsPerSubspace)))
        }
        check(res.imaginary.value.toDouble() < 1e-5)
        return res.real.value.toDouble()
    }

    /**
     * Calculates the IP between
     * the approximation specified with the index and the supplied vector which was permuted with the same permutation
     * that was applied to the data when creating this [PQ] object.
     */
    fun approximateAsymmetricIP(sigi: IntArray, v: VectorValue<*>): NumericValue<*> {
        require(v.logicalSize == numSubspaces * dimensionsPerSubspace)
        var res = codebooks[0].centroids[sigi[0]].dot(v, 0, 0, dimensionsPerSubspace)
        for (k in 1 until numSubspaces) {
            val centi = codebooks[k].centroids[sigi[k]]
            res += centi.dot(v, 0, k * dimensionsPerSubspace, dimensionsPerSubspace)
        }
        return res
    }

    /**
     * Builds the signature, which is the representative centroid in each subspace for the specified vector (needs
     * to be permuted the same way that this [PQ] object was built!
     */
    fun getSignature(v: VectorValue<*>): IntArray {
        require(v.logicalSize == numSubspaces * dimensionsPerSubspace)
        return IntArray(numSubspaces) { k ->
            codebooks[k].quantizeVector(v, k * dimensionsPerSubspace, dimensionsPerSubspace)
        }
    }

    fun precomputeCentroidQueryIP(permutedQuery: DoubleArray): PQCentroidQueryIP {
        TODO()
//        return PQCentroidQueryIP(Array(numSubspaces) { k ->
//            DoubleArray(numCentroids) { i ->
//                var ip = 0.0
//                for (j in 0 until dimensionsPerSubspace) {
//                    ip += permutedQuery[k * dimensionsPerSubspace + j] * codebooks[k].centroids[i][j]
//                }
//                ip
//            }
//        }
//        )
    }

    fun precomputeCentroidQueryIPComplexVectorValue(query: ComplexVectorValue<*>): PQCentroidQueryIPComplexVectorValue {
        return PQCentroidQueryIPComplexVectorValue(Array(numSubspaces) { k ->
            Complex32VectorValue(Array(numCentroids) { i ->
                query.dot(codebooks[k].centroids[i], k * dimensionsPerSubspace, 0, dimensionsPerSubspace).asComplex32()
            })
        })
    }


    fun precomputeCentroidQueryRealIPFloat(query: Complex32VectorValue): PQCentroidQueryIPFloat {
        return PQCentroidQueryIPFloat(Array(numSubspaces) { k ->
            FloatArray(numCentroids) { i ->
                query.real().dot(codebooks[k].centroids[i], k * dimensionsPerSubspace, 0, dimensionsPerSubspace).real.value.toFloat()
            }
        }
        )
    }

    fun precomputeCentroidQueryImagIPFloat(query: Complex32VectorValue): PQCentroidQueryIPFloat {
        return PQCentroidQueryIPFloat(Array(numSubspaces) { k ->
            FloatArray(numCentroids) { i ->
                query.imaginary().dot(codebooks[k].centroids[i], k * dimensionsPerSubspace, 0, dimensionsPerSubspace).value.toFloat()
            }
        }
        )
    }
}
