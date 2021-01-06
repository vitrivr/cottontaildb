package org.vitrivr.cottontail.database.index.pq

import org.apache.commons.math3.linear.*
import org.apache.commons.math3.ml.clustering.CentroidCluster
import org.apache.commons.math3.ml.clustering.Clusterable
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer
import org.apache.commons.math3.random.JDKRandomGenerator
import org.apache.commons.math3.stat.correlation.Covariance

import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.*

import kotlin.IllegalArgumentException
import kotlin.math.absoluteValue

/**
 * Class representing a codebook for a single subspace for Product Quantization (PQ). The codebook contains the [PQCodebook]
 * (real valued vectors) for the subspace [dataCovarianceMatrix] is the covariance matrix that was used for learning the
 * codebook. Entries in the array are column-vectors of that matrix.
 *
 * @author Gabriel Zihlmann
 * @version 1.0.0
 */
class PQCodebook<T: VectorValue<*>> (val centroids: Array<T>, val dataCovarianceMatrix: Array<T>) {
    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(PQCodebook::class.java)

        /**
         * @param subspaceData entries in this array are subvectors of the data to index
         */
        fun learnFromRealData(subspaceData: Array<DoubleArray>, dataCovMatrix: RealMatrix, numCentroids: Int, maxIterations: Int, seed: Long): Pair<PQCodebook<DoubleVectorValue>, IntArray> {
            val (centroidClusters, signatures) = clusterRealData(subspaceData, dataCovMatrix, numCentroids, maxIterations, seed)
            return PQCodebook(Array(numCentroids) {
                DoubleVectorValue(centroidClusters[it].center.point)
            }, dataCovMatrix.data.map { DoubleVectorValue(it) }.toTypedArray()) to signatures
        }

        /**
         * @param subspaceData entries in this array are subvectors of the data to index
         */
        fun learnFromRealData(subspaceData: Array<DoubleArray>, numCentroids: Int, maxIterations: Int, seed: Long): Pair<PQCodebook<DoubleVectorValue>, IntArray> {
            val dataCovMatrix = Covariance(subspaceData, false).covarianceMatrix
            return learnFromRealData(subspaceData, dataCovMatrix, numCentroids, maxIterations, seed)
        }

        /**
         * Internally, for real valued data, the clustering is done with apache commons k-means++ in double precision
         * but the returned codebook contains centroids of the same datatype as was supplied
         * @param subspaceData entries in this array are subvectors of the data to index
         */
        fun learnFromRealData(subspaceData: Array<out RealVectorValue<*>>, numCentroids: Int, maxIterations: Int, seed: Long): Pair<PQCodebook<out RealVectorValue<*>>, IntArray> {
            LOGGER.debug("Calculating data covariance matrix from supplied data.")
            require(subspaceData.all { it::class.java == subspaceData[0]::class.java })
            return when(subspaceData[0]) {
                is DoubleVectorValue -> {
                    val data = Array(subspaceData.size) {
                        (subspaceData[it] as DoubleVectorValue).data
                    }
                    learnFromRealData(data, numCentroids, maxIterations, seed)
                }
                is FloatVectorValue -> {
                    val array = Array(subspaceData.size) {
                        (subspaceData[it] as FloatVectorValue).data.map { j -> j.toDouble() }.toDoubleArray()
                    }
                    val (doublePQCodebook, signatures) = learnFromRealData(array, numCentroids, maxIterations, seed)
                    val floatPQCodebooks = PQCodebook(
                            doublePQCodebook.centroids.map { c -> FloatVectorValue(FloatArray(c.logicalSize) { c.data[it].toFloat()}) }.toTypedArray(),
                            doublePQCodebook.dataCovarianceMatrix.map { col -> FloatVectorValue(FloatArray(col.logicalSize) { col.data[it].toFloat()}) }.toTypedArray())
                    floatPQCodebooks to signatures
                }
                else -> throw IllegalArgumentException("Other RealVectorValue types not implemented for PQ")
            }
        }

        private fun clusterRealData(subspaceData: Array<DoubleArray>, dataCovMatrix: RealMatrix, numCentroids: Int, maxIterations: Int, seed: Long): Pair<MutableList<CentroidCluster<Vector>>, IntArray> {
            val measure: (a: DoubleArray, b: DoubleArray) -> Double = { a, b ->
                mahalanobisSqOpt(a, 0, a.size, b, 0, dataCovMatrix)
            }
            val clusterer = KMeansPlusPlusClusterer<Vector>(numCentroids, maxIterations, measure, JDKRandomGenerator(seed.toInt()))
            LOGGER.debug("Learning...")
            val centroidClusters = clusterer.cluster(subspaceData.mapIndexed { i, value ->
                Vector(value, i)
            })
            LOGGER.debug("Done learning.")
            LOGGER.debug("Building codebook and signatures from commons math result")
            val signatures = IntArray(subspaceData.size)
            centroidClusters.forEachIndexed { i, cluster ->
                cluster.points.forEach {
                    signatures[it.index] = i
                }
            }
            return centroidClusters to signatures
        }

        fun learnFromComplexData(subspaceData: Array<out ComplexVectorValue<out Number>>, numCentroids: Int, maxIterations: Int, seed: Long): Pair<PQCodebook<out ComplexVectorValue<*>>, IntArray> {
            val dataCovMatrixCommons = complexCovarianceMatrix(subspaceData)
            val dataCovMatrix = fieldMatrixToVectorArray(dataCovMatrixCommons, subspaceData[0]::class)
//            val (signatures, centroids) = clusterComplexData(numCentroids, dataCovMatrix, subspaceData, maxIterations)
            val (signatures, centroids) = clusterComplexDataPlusPlus(numCentroids, dataCovMatrix, subspaceData, maxIterations, seed)
            return when (subspaceData[0]) {
                is Complex32VectorValue -> {
                    PQCodebook(centroids.map{ it as Complex32VectorValue}.toTypedArray(), dataCovMatrix.map{ it as Complex32VectorValue}.toTypedArray()) to signatures
                }
                is Complex64VectorValue -> {
                    PQCodebook(centroids.map { it as Complex64VectorValue }.toTypedArray(), dataCovMatrix.map { it as Complex64VectorValue }.toTypedArray()) to signatures
                }
                else -> error("Unsupported type ${subspaceData[0]::class}")
            }
        }

        /**
         * @param dataCovMatrix is an array of column-vectors of the non-centered data covariance matrix
         */
        private fun clusterComplexDataPlusPlus(numCentroids: Int, dataCovMatrix: Array<out ComplexVectorValue<*>>, subspaceData: Array<out ComplexVectorValue<*>>, maxIterations: Int, seed: Long): Pair<IntArray, Array<out ComplexVectorValue<*>>> {
            // to decouple from the implementation of [Complex64VectorValue] or [Complex32VectorValue] we need copy the
            // data to a DoubleArray with a layout that we decide...
            val subspaceDataDoubles = subspaceData.map { v ->
                DoubleArray(v.logicalSize shl 1) { i ->
                    if (i % 2 == 0) v[i / 2].real.value.toDouble() else v[i / 2].imaginary.value.toDouble()
                }
            }.toTypedArray()
            // do same for matrix. But it will be more efficient later on if we have it as row vectors
//            val dataCovMatrixDoubles = dataCovMatrix.map { v ->
//                DoubleArray(v.logicalSize shl 1) { i ->
//                    if (i % 2 == 0) v[i / 2].real.value.toDouble() else v[i / 2].imaginary.value.toDouble()
//                }
//            }.toTypedArray()
            val dataCovMatrixRowVectors = Array(dataCovMatrix[0].logicalSize) { row ->
                DoubleArray(dataCovMatrix.size shl 1) { colComponent ->
                    if (colComponent % 2 == 0) dataCovMatrix[colComponent / 2][row].real.value.toDouble() else dataCovMatrix[colComponent / 2][row].imaginary.value.toDouble()
                }
            }
            val dist: (a: DoubleArray, b: DoubleArray) -> Double = { a, b ->
                require(a.size == b.size)
                // d = (a-b)^T*cov^(-1)*(a-b)
                // or is it d = (a-b)^H*cov^(-1)*(a-b)??
                val diff = DoubleArray(a.size) {
                    a[it] - b[it]
                }
                var dReal = 0.0
                var dImag = 0.0
                for (mRow in dataCovMatrixRowVectors.indices) {
                    var hReal = 0.0
                    var hImag = 0.0
                    for(vCol in 0 until diff.size / 2) { // iterate vector components (1/2 because each component is 2 elements (real + imag)
                        // now do basically complex version of what's happening in mahalanobisSqOpt()
                        hReal += dataCovMatrixRowVectors[mRow][vCol * 2] * diff[vCol * 2] + dataCovMatrixRowVectors[mRow][vCol * 2 + 1] * diff[vCol * 2 + 1] // case with conjugation. Without it, dist is not real!
                        hImag += dataCovMatrixRowVectors[mRow][vCol * 2 + 1] * diff[vCol * 2] - dataCovMatrixRowVectors[mRow][vCol * 2] * diff[vCol * 2 + 1]
                    }
                    dReal += diff[mRow * 2] * hReal - diff[mRow * 2 + 1] * hImag
                    dImag += diff[mRow * 2 + 1] * hReal + diff[mRow * 2] * hImag // todo: drop this...?
                }
                check(dImag.absoluteValue < 1e-5) {"Distance should be real but imaginary part was $dImag"}
                check(dReal >= 0) {"Distance must be >= 0 but was $dReal"}
                dReal
            }
            val distAbsIP: (a: DoubleArray, b: DoubleArray) -> Double = { a, b ->
                //make a distance that returns 1 - abs(a.dot(b)) (a, b are normalized)
                val a_ = Complex64VectorValue(Array(a.size / 2) {
                    Complex64Value(a[it * 2], a[it * 2 + 1])
                })
                val b_ = Complex64VectorValue(Array(b.size / 2) {
                    Complex64Value(b[it * 2], b[it * 2 + 1])
                })
                val d = 1.0 - a_.dot(b_).abs().value.toDouble()
                check(d >= -1e-5) {"Distance must be >= 0 but was $d"}
                d.coerceAtLeast(0.0)
            }
            val c = KMeansPlusPlusClusterer<Vector>(numCentroids, maxIterations, dist, JDKRandomGenerator(seed.toInt()))
//            val c = KMeansPlusPlusClusterer<Vector>(numCentroids, maxIterations, distAbsIP, JDKRandomGenerator(seed.toInt())) // this guy doesn't converge...
            val clusterResults = c.cluster(subspaceDataDoubles.mapIndexed { i, v -> Vector(v, i) })
            val signatures = IntArray(subspaceData.size)
            clusterResults.forEachIndexed { i, clusterCenter ->
                clusterCenter.points.forEach { v ->
                    signatures[v.index] = i
                }
            }
            val centers = when(subspaceData[0]) {
                is Complex32VectorValue -> {
                    clusterResults.map { cCenter ->
                        Complex32VectorValue(Array (cCenter.center.point.size / 2) {
                            Complex32Value(cCenter.center.point[it * 2], cCenter.center.point[it * 2 + 1])
                            }
                        )
                    }.toTypedArray()
                }
                is Complex64VectorValue -> {
                    clusterResults.map { cCenter ->
                        Complex64VectorValue(Array (cCenter.center.point.size / 2) {
                            Complex64Value(cCenter.center.point[it * 2], cCenter.center.point[it * 2 + 1])
                            }
                        )
                    }.toTypedArray()
                }
                else -> throw IllegalArgumentException("Unsupported type ${subspaceData[0]::class}")
            }
            return signatures to centers
        }

        private fun mahalanobisSqOpt(a: DoubleArray, aStart: Int, length: Int, b: DoubleArray, bStart: Int, dataCovMatrix: RealMatrix): Double {
            require(dataCovMatrix.columnDimension == length)
            require(dataCovMatrix.rowDimension == length)
            var dist = 0.0
            val diff = DoubleArray(length) {
                a[aStart + it] - b[bStart + it]
            }
            for (i in 0 until dataCovMatrix.columnDimension) {
                var h = 0.0
                for (j in diff.indices) {
                    h += diff[j] * dataCovMatrix.getEntry(i, j)
                }
                dist += h * diff[i]
            }
            return dist
        }

        /**
         * todo: check efficiency of this. It will probably be used A LOT
         *       is this correct for complex case? dot includes a conjugate. Normal matrix multiplication not
         * @param dataCovMatrix is a collection of column-vectors
         */
        private fun mahalanobisSqOpt(a: VectorValue<*>, aStart: Int, length: Int, b: VectorValue<*>, bStart: Int, dataCovMatrix: Array<out VectorValue<*>>): Double {
            require(dataCovMatrix.size == length)
            require(dataCovMatrix[0].logicalSize == length)
            var dist: NumericValue<*> = when (val t = a::class.java) {
                Complex32VectorValue::class.java -> Complex32Value(FloatArray(2))
                Complex64VectorValue::class.java -> Complex64Value(DoubleArray(2))
                DoubleVectorValue::class.java -> DoubleValue(0.0)
                FloatVectorValue::class.java -> FloatValue(0.0f)
                else -> error("Unknown type '$t'")
            }

            val diff = a.minus(b, aStart, bStart, length)
            for (i in dataCovMatrix.indices) {
                val ip = dataCovMatrix[i].dot(diff)
                val v = diff[i] * ip
                dist += v
            }
            check(dist.imaginary.value.toDouble().absoluteValue < 1e-5)
            return dist.real.value.toDouble()
        }

    }

    init {
        require(centroids.all { c -> c.logicalSize == centroids[0].logicalSize && centroids[0]::class.java == c::class.java})
        require(centroids[0].logicalSize == dataCovarianceMatrix.size)
        require(dataCovarianceMatrix[0].logicalSize == dataCovarianceMatrix.size
                && dataCovarianceMatrix.all {
                    it.logicalSize == dataCovarianceMatrix[0].logicalSize
                    && it::class.java == dataCovarianceMatrix[0]::class.java
                }){ "The data covariance matrix must be square and all entries must be of the" +
                    "same type!" }
        require(dataCovarianceMatrix.indices.all { dataCovarianceMatrix[it][it].imaginary.value.toDouble() < 1.0e-5 })
            { "Data covariance matrix does not have a real diagonal!" }
        // todo: do better test to actually test for being hermitian, not just whether it's square...
//        require(isSymmetric(dataCovarianceMatrix, 1e-5))
    }

    /**
     * returns the centroid index to which the supplied vector is quantized.
     * supplied vector v can be the full vector covering multiple subspaces
     * in this case start and length should be specified to indicate the range
     * of the subspace of this [PQCodebook]
     */
    fun quantizeVector(v: VectorValue<*>, start: Int = 0, length: Int = v.logicalSize): Int {
        return smallestMahalanobis(v, start, length)
    }


    /**
     * Will return the centroid index in the codebook to which the supplied vector has the smallest
     * mahalanobis distance
     */
    private fun smallestMahalanobis(v: VectorValue<*>, start: Int, length: Int): Int {
        require(length == centroids[0].logicalSize)
        var mahIndex = 0
        var mah = Double.POSITIVE_INFINITY
        centroids.forEachIndexed { i, c ->
            val m = mahalanobisSqOpt(c, 0, length, v, start, dataCovarianceMatrix)
            if (m < mah) {
                mah = m
                mahIndex = i
            }
        }
        return mahIndex

    }

    /**
     * returns a list of lists of strings. First element is header of data as list of strings,
     * following elements are the centroid data
     */
    fun centroidsToString(): List<List<String>> {
        return when (val firstC = centroids[0]) {
            is RealVectorValue<*> -> {
                val header = listOf("Index") + firstC.indices.map { "component$it" }
                val data = centroids.mapIndexed { i, c ->
                    listOf("$i") + (c as RealVectorValue<*>).map { it.value.toDouble().toString() }
                }
                check(header.size == data[0].size)
                listOf(header) + data
            }
            is ComplexVectorValue<*> -> {
                val header = listOf("Index") + firstC.indices.flatMap { listOf("component${it}Real", "component${it}Imag") }
                val data = centroids.mapIndexed { i, c ->
                    listOf("$i") + (c as ComplexVectorValue<*>).flatMap { component ->
                        listOf(component.real, component.imaginary).map { it.value.toDouble().toString() }
                    }
                }
                check(header.size == data[0].size)
                listOf(header) + data
            }
            else -> {
                error("unexpected type ${firstC::class}")
            }
        }
    }
}

private class Vector(val data: DoubleArray, val index: Int): Clusterable {
    override fun getPoint() = this.data
}
