package ch.unibas.dmi.dbis.cottontail.database.index.va.vaplus

import kotlin.math.pow

/**
 * This class is a marks generator.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
object MarksGenerator {

    /**
     * Get marks.
     * @param dataSample The data sample.
     * @param numberOfMarks The number of marks of every dimension.
     * @return An [Array] containing the marks of every dimension.
     * TODO
     */
    fun getMarks(dataSample: Array<DoubleArray>, numberOfMarks: List<Int>): Array<MutableList<Double>> {
        val init = createEquiDistanceMarks(dataSample, numberOfMarks).map {
            it.add(it.last() + 10E-9) // epsilon
            it
        }.toTypedArray()
        /**
         * Iterate over dimensions.
         * Get marks of d-th dimension.
         * Do k-means.
         */
        numberOfMarks.indices.map { d ->
            var marks = init[d]
            var delta: Double
            var deltaBar = Double.MAX_VALUE
            do {
                // k-means
                delta = deltaBar
                // pseudocode line 3
                /**
                 * Iterate over marks of d-th dimension.
                 * Iterate over data.
                 * Check if data point is in interval of marks.
                 * Calculate mean of data points in interval (rj).
                 * Return list of mean of every interval in d (rjs).
                 */
                val rjs = (0 until marks.size - 1).map { c ->
                    var rj = 0.0
                    var count = 0
                    dataSample.forEach {
                        if (it[d] >= marks[c] && it[d] < marks[c + 1]) {
                            rj += it[d]
                            count += 1
                        }
                    }
                    rj /= count // mean
                    rj
                }
                // pseudocode line 7
                /**
                 * Iterate over marks of d-th dimension.
                 * Adjust marks (moving along distance, no long equidistance)
                 * The mark at position c is adjusted with "(first mean value + second mean value) / 2"
                 */
                (1 until marks.size - 1).forEach { c ->
                    marks[c] = (rjs[c - 1] + rjs[c]) / 2
                }
                // pseudocode line 8
                /**
                 * Iterate over marks of d-th dimension.
                 * Iterate over data.
                 * Check if data point is in interval of (new) marks.
                 * If so, apply formula:
                 * tmp = (difference between data point and c).pow(2) = euclidean distance
                 * if distance > 0.999 then break
                 */
                deltaBar = (0 until marks.size - 1).map { c ->
                    var tmp = 0.0
                    dataSample.forEach {
                        if (it[d] >= marks[c] && it[d] < marks[c + 1]) {
                            tmp += (it[d] - rjs[c]).pow(2)
                        }
                    }
                    tmp
                }.sum()
            } while (deltaBar / delta < 0.999)
        }
        return init
    }

    /**
     * Create marks per dimension (equally spaced).
     *
     * @param dataSample The data sample.
     * @param numberOfMarks The number of marks per dimension.
     * @return An [Array] containing the marks of every dimension.
     */
    private fun createEquiDistanceMarks(dataSample: Array<DoubleArray>, numberOfMarks: List<Int>): Array<MutableList<Double>> {
        val min = getMin(dataSample)
        val max = getMax(dataSample)
        return Array(min.size) { it ->
            MutableList(numberOfMarks[it] * 2) {
                it * (max[it] - min[it]) / (numberOfMarks[it] - 1)
            }
        }
    }

    /**
     * Get vector ([DoubleArray]) which values are minimal.
     *
     * @param dataSample The data sample.
     * @return A [DoubleArray] which is the minimum from dataSample.
     */
    private fun getMin(dataSample: Array<DoubleArray>): DoubleArray = dataSample.fold(DoubleArray(dataSample.first().size) { Double.MAX_VALUE }) { a, b ->
        DoubleArray(a.size) { minOf(a[it], b[it]) }
    }

    /**
     * Get vector ([DoubleArray]) which values are maximal.
     *
     * @param dataSample The data sample.
     * @return A [DoubleArray] which is the maximum from dataSample.
     */
    private fun getMax(dataSample: Array<DoubleArray>): DoubleArray = dataSample.fold(DoubleArray(dataSample.first().size) { Double.MIN_VALUE }) { a, b ->
        DoubleArray(a.size) { maxOf(a[it], b[it]) }
    }

}
