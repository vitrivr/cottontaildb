package ch.unibas.dmi.dbis.cottontail.database.index.va.vaplus

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.VectorValue
import org.apache.commons.math3.linear.EigenDecomposition
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.linear.RealMatrix
import org.apache.commons.math3.stat.correlation.Covariance
import java.io.Serializable
import kotlin.math.*

/**
 * This class implements the necessary methods for a VA+ index structure (see H. Ferhatosmanoglu, E. Tuncel, D. Agrawal,
 * A. El Abbadi (2006): High dimensional nearest neighbor searching. Information Systems).
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class VAPlus : Serializable {

    private val totalNumberOfBits = 32 // 64, 128, 256, 1024 // TODO

    /* Indexing */
    /**
     * This method generates a sample subset from the data.
     *
     * @param data  A [Recordset] containing all data.
     * @param size  The size of the data sample.
     * @return  An subset from data.
     */
    fun getDataSample(data: Recordset, column: ColumnDef<*>, size: Int): Array<DoubleArray> {
        val p = size / data.rowCount.toDouble()
        val dataSample = ArrayList<DoubleArray>(size + 100)
        data.map {
            if (Math.random() >= 1.0 - p) {
                dataSample.add(convertToDoubleArray(it[column] as VectorValue<*>))
            }
        }
        return dataSample.toTypedArray()
    }

    /**
     * "VA-file in KLT domain"
     * (see paper page 515-517)
     *
     * This method does a transformation into the KLT domain, in the paper described as "strip off all the correlation
     * between the dimensions of feature vectors", following the steps below:
     *
     * - Create a [RealMatrix] from data.
     * - Create a [Covariance] from [RealMatrix].
     * - Create a covariance matrix, cBar.
     * - Create a [EigenDecomposition] from cBar.
     * - Create an empty KLT matrix, k.
     * - Get eigenVector from column in cBar.
     * - In this column, fill the row with the corresponding eigenVector.
     * - Transpose k to kBar.
     * - Create a [RealMatrix], dataMatrix, of every column in data.
     * - Multiply kBar with dataMatrix.
     *
     * @param dataSample An [Array<DoubleArray>] containing data samples.
     * @return Pair of dataSample in KLT domain, and kBar.
     */
    fun transformToKLTDomain(dataSample: Array<DoubleArray>): Pair<Array<DoubleArray>, RealMatrix> {
        // TODO Remember to mean-center data before PCA
        val cBar = Covariance(MatrixUtils.createRealMatrix(dataSample)).covarianceMatrix
        val eigenDecomposition = EigenDecomposition(cBar)
        val k = MatrixUtils.createRealMatrix(40, 40)
        for (column in 0 until cBar.columnDimension) {
            val eigenVector = eigenDecomposition.getEigenvector(column)
            for (row in 0 until cBar.rowDimension) {
                k.setEntry(row, column, eigenVector.getEntry(row))
            }
        }
        val kBar = k.transpose()
        dataSample.forEachIndexed { index, element ->
            val dataMatrix = MatrixUtils.createRealMatrix(arrayOf(element))
            dataSample[index] = kBar.multiply(dataMatrix.transpose()).getColumnVector(0).toArray()
        }
        return Pair(dataSample, kBar)
    }

    /**
     * "Non-uniform bit allocation"
     * (see paper page 517-518)
     *
     * This method allocates bits to every dimension in a non-uniform way.
     *
     * @param dataSample The data sample.
     * @param dimension Dimension of the data.
     * @return An [IntArray] containing the number of bits per dimension.
     */
    fun nonUniformBitAllocation(dataSample: Array<DoubleArray>, dimension: Int): IntArray {
        val mean = DoubleArray(dimension)
        val variance = DoubleArray(dimension)
        for (i in 0 until dimension) {
            for (index in dataSample.indices) {
                mean[i] += dataSample[index][i]
            }
            mean[i] = mean[i] / dataSample.size
        }
        for (i in 0 until dimension) {
            for (index in dataSample.indices) {
                variance[i] += (dataSample[index][i] - mean[i]).pow(2)
            }
            variance[i] = mean[i] / dataSample.size
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
     * "Non-uniform quantization"
     * (see paper page 518)
     *
     * This method performs a non-uniform quantization.
     *
     * @param dataSample The data sample.
     * @param b The number of bits per dimension.
     */
    fun nonUniformQuantization(dataSample: Array<DoubleArray>, b: IntArray): Pair<Array<MutableList<Double>>, SignatureGenerator> {
        val numberOfMarks = b.map {
            minOf(maxOf(1, 2 shl (it - 1)), Short.MAX_VALUE.toInt())
        }
        val marks = MarksGenerator.getMarks(dataSample, numberOfMarks)
        val signatureGenerator = SignatureGenerator(b)
        return Pair(marks, signatureGenerator)
    }

    /**
     * This methods calculates the signature of the vector.
     * This method checks for every mark if the corresponding vector is beyond or not.
     * If so, mark before is the corresponding mark.
     *
     * @param vector The vector.
     * @param marks The marks.
     * @return An [IntArray] containing the signature of the vector.
     */
    fun getSignature(vector: DoubleArray, marks: Array<MutableList<Double>>): IntArray = IntArray(vector.size) {
        val index = marks[it].indexOfFirst { i -> i >= vector[it] }
        if (index == -1) {
            marks[it].size - 2
        } else {
            index - 1
        }
    }

    /**
     * This helper method convert vector to double array.
     *
     * @param vector A [VectorValue].
     * @return A [DoubleArray] with the values from value.
     */
    fun convertToDoubleArray(vector: VectorValue<*>): DoubleArray = DoubleArray(vector.size * 2) { vector.getAsDouble(it) }

    /**
     * This helper method returns the index of the maximal value in an array.
     *
     * @param array The [DoubleArray].
     * @return An [Int] denoting the index of the maximal value.
     */
    private fun getMaxIndex(array: DoubleArray): Int {
        var maxValue = Double.MIN_VALUE
        var maxIndex = -1
        for (index in array.indices) {
            var element = array[index]
            if (element > maxValue) {
                maxValue = element
                maxIndex = index
            }
        }
        return maxIndex
    }

    /* Querying */
    /**
     * This method performs a scan.
     */
    fun scan(query: FloatArray, marks: Array<List<Double>>) {
        val bounds = computeBounds(query, marks)
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
    }

    private fun computeBounds(query: FloatArray, marks: Array<List<Double>>): Pair<Array<DoubleArray>, Array<DoubleArray>>? {
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
            while (it.hasNext()) {
                val dimMark = it.next()
                //val d0fv1 = distance.element(dimMark(0), fvi)
                //val d1fv1 = distance.element(dimMark(1), fvi)
                //if (fvi < dimMark(0)) {
                //    lbounds[i][j] = d0fv1
                //} else if (fvi > dimMark(1)) {
                //    lbounds[i][j] = d1fv1
                //}
                //if (fvi <= (dimMark(0) + dimMark(1)) / 2.toFloat()) {
                //    ubounds[i][j] = d1fv1
                //} else {
                //    ubounds[i][j] = d0fv1
                //}
                j += 1
            }
            i += 1
        }
        return null//Pair(lbounds, ubounds)
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
