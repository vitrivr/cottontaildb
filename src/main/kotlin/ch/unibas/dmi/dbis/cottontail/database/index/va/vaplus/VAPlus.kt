package ch.unibas.dmi.dbis.cottontail.database.index.va.vaplus

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.VectorValue
import org.apache.commons.math3.linear.EigenDecomposition
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.linear.RealMatrix
import org.apache.commons.math3.stat.correlation.Covariance
import java.io.Serializable
import kotlin.math.*


/**
 * see H. Ferhatosmanoglu, E. Tuncel, D. Agrawal, A. El Abbadi (2006): High dimensional nearest neighbor searching. Information Systems.
 */
class VAPlus : Serializable {
    private val totalNumberOfBits = 32 // 32, 64, 128, 256, 1024 // TODO
    private val numberOfDimensions = intArrayOf(1)
    private val distance = 1

    /* Indexing */
    /**
     * This method generates a data sample out of data.
     */
    fun getDataSample(data: Array<DoubleArray>, size: Int): Array<DoubleArray> {
        // TODO create data sample of size
        // ADAMpro > IndexGenerator > 48
        return data
    }

    /**
     * VA-file in KLT domain
     *
     * "Strip off all the correlation between the dimensions of feature vectors."
     *
     * TODO Remember to mean-center data before PCA
     * Create [RealMatrix] from data.
     * Create [Covariance] from real matrix.
     * Create covariance cBar matrix from covariance.
     * Create [EigenDecomposition] from cBar.
     * Create (empty) KLT-matrix k.
     * Get Eigenvector of every column in cBar.
     * Fill k (row by row, column for column) with Eigenvectors.
     * Get every column of data as dataMatrix.
     * Transpose k to k', multiply with dataMatrix (and get column).
     *
     * @return Diagonalization of the autocovariance matrix cBar
     */
    fun transformToKLTDomain(data: Array<DoubleArray>): Array<DoubleArray> {
        val cBar = Covariance(MatrixUtils.createRealMatrix(data)).covarianceMatrix
        val eigenDecomposition = EigenDecomposition(cBar)
        val kltMatrix = MatrixUtils.createRealMatrix(40, 40)
        for (column in 0 until cBar.columnDimension) {
            val eigenVector = eigenDecomposition.getEigenvector(column)
            for (row in 0 until cBar.rowDimension) {
                kltMatrix.setEntry(row, column, eigenVector.getEntry(row))
            }
        }
        data.forEachIndexed { index, element ->
            val dataMatrix = MatrixUtils.createRealMatrix(arrayOf(element))
            val tmp = kltMatrix.transpose().multiply(dataMatrix.transpose()).getColumnVector(0).toArray()
            data[index] = tmp
        }
        return data
    }

    /**
     * Non-uniform bit allocation
     */
    fun nonUniformBitAllocation(dimension: Int, data: Array<DoubleArray>): IntArray {
        val mean = DoubleArray(dimension)
        val variance = DoubleArray(dimension)
        for (i in 0 until dimension) {
            for (index in data.indices) {
                mean[i] += data[index][i]
            }
            mean[i] = mean[i] / data.size
        }
        for (i in 0 until dimension) {
            for (index in data.indices) {
                variance[i] += (data[index][i] - mean[i]).pow(2)
            }
            variance[i] = mean[i] / data.size
        }
        // Weber / Böhm (2000): Trading Quality for Time with Nearest Neighbor Search
        val b0 = totalNumberOfBits
                ?: (dimension * maxOf(5, ceil(5 + 0.5 * log(dimension.toDouble() / 10, E) / log(2.0, E)).toInt()))
        var k = 0
        val b = IntArray(dimension) { 0 }
        while (k < b0) {
            val j = getMaxIndex(variance)
            b[j] += 1
            variance[j] = variance[j] / 4
            k += 1
        }
        return b
    }

    /**
     * Non-uniform quantization
     */
    fun nonUniformQuantization(data: Array<DoubleArray>, b: IntArray) {
        val maxMarks = b.map {
            minOf(maxOf(1, 2 shl (it - 1)), Short.MAX_VALUE.toInt())
        }
        val signatureGenerator = SignatureGenerator(b)
        val marks = MarksGenerator.getMarks(data, maxMarks)
        // TODO
    }

    /**
     * returns the max index of the variance array
     */
    private fun getMaxIndex(variances: DoubleArray): Int {
        var maxValue = Double.MIN_VALUE
        var maxIndex = -1
        for (index in variances.indices) {
            var variance = variances[index]
            if (variance > maxValue) {
                maxValue = variance
                maxIndex = index
            }
        }
        return maxIndex
    }

    /* Querying */
    fun scan(entity: Entity, query: VectorValue<FloatArray>, recordset: Recordset): Recordset {
        //val bounds = computeBounds(query, VAPlusMarksGenerator().getMarks(entity, listOf(2)))
        //val (lbIndex, lbBounds) = compressBounds(bounds.first)
        //val (ubIndex, ubBounds) = compressBounds(bounds.second)

        /**
        val cellsDistUDF = (boundsIndexBc: Broadcast[CompressedBoundIndex], boundsBoundsBc: Broadcast[CompressedBoundBounds]) => udf((cells: Seq[Short]) => {
        var bound: Distance = 0
        var idx = 0
        while (idx < cells.length) {
        val cellsIdx = if (cells(idx) < 0) {
        (Short.MaxValue + 1) * 2 + cells(idx)
        } else {
        cells(idx)
        }
        bound += boundsBoundsBc.value(boundsIndexBc.value(idx) + cellsIdx)
        idx += 1
        }

        bound
        })
        import ac.spark.implicits._
        val boundedData = data
        .withColumn("ap_lbound", cellsDistUDF(lbIndexBc, lbBoundsBc)(col(AttributeNames.featureIndexColumnName)))
        .withColumn("ap_ubound", cellsDistUDF(ubIndexBc, ubBoundsBc)(col(AttributeNames.featureIndexColumnName))) //note that this is computed lazy!
        val pk = this.pk.name.toString
        //local filtering
        val localRes = boundedData.coalesce(ac.config.defaultNumberOfPartitionsIndex)
        .mapPartitions(pIt => {
        //in here  we compute for each partition the k nearest neighbours and collect the results
        val localRh = new VAResultHandler(k)

        while (pIt.hasNext) {
        val current = pIt.next()
        localRh.offer(current, pk)
        }

        localRh.results.iterator
        })
        /*import ac.spark.implicits._
        val minUpperPart = localRes
        .mapPartitions(pIt => Seq(pIt.maxBy(_.ap_upper)).iterator).agg(min("ap_upper")).collect()(0).getDouble(0)
        val res = localRes.filter(_.ap_lower <= minUpperPart).toDF()*/
        val res = if (ac.config.vaGlobalRefinement || options.get("vaGlobal").map(_.toBoolean).getOrElse(false)) {
        // global refinement
        val globalRh = new VAResultHandler(k)
        val gIt = localRes.collect.iterator

        while (gIt.hasNext) {
        val current = gIt.next()
        globalRh.offer(current, pk)
        }
        ac.sqlContext.createDataset(globalRh.results).toDF()
        } else {
        localRes.toDF()
        }
        res
         */

        return recordset
    }

    private fun computeBounds(query: VectorValue<FloatArray>, marks: List<List<Double>>): Pair<Array<DoubleArray>, Array<DoubleArray>>? {
        //  val lbounds, ubounds = Array.tabulate(marks.length)(i => Array.ofDim[Distance](math.max(0, marks(i).length - 1)))
        val (lbounds, ubounds) = arrayOf(DoubleArray(marks.size))
        for (index in marks.indices) {
            lbounds[index] = maxOf(0, marks[index].size - 1).toDouble()
            ubounds[index] = maxOf(0, marks[index].size - 1).toDouble()
        }
        var i = 0
        while (i < marks.size) {
            val dimMarks = marks[i]
            val fvi = query[i]
            var j = 0
            val it = dimMarks.iterator() // TODO windowed(2)? --> sliding(2)
            /*while (it.hasNext()) {
                val dimMark = it.next()
                val d0fv1 = distance.element(dimMark(0), fvi)
                val d1fv1 = distance.element(dimMark(1), fvi)
                if (fvi < dimMark(0)) {
                    lbounds[i][j] = d0fv1
                } else if (fvi > dimMark(1)) {
                    lbounds[i][j] = d1fv1
                }
                if (fvi <= (dimMark(0) + dimMark(1)) / 2.toFloat()) {
                    ubounds[i][j] = d1fv1
                } else {
                    ubounds[i][j] = d0fv1
                }
                j += 1
            }*/
            i += 1
        }
        return null //Pair(lbounds, ubounds)
    }

    private fun compressBounds(bounds: Array<DoubleArray>): Pair<IntArray, FloatArray>? {
        val lengths = bounds.map { it.size }
        val totalLength = lengths.sum()
        val cumLengths = {
            // compute cumulative sum of lengths
            val cumLengths = IntArray(lengths.size)
            var i = 0
            var cumSum = 0
            while (i < lengths.size - 1) {
                cumSum += lengths[i]
                cumLengths[i + 1] = cumSum
                i += 1
            }
            cumLengths
        }
        val newBounds = {
            val newBounds = FloatArray(totalLength)
            var i = 0
            var j: Int
            while (i < lengths.size) {
                j = 0
                while (j < lengths[i]) {
                    //newBounds[cumLengths[i] + j] = bounds[i][j].toFloat()
                    j += 1
                }
                i += 1
            }
            newBounds
        }
        return null //Pair(cumLengths, newBounds)
    }

}
