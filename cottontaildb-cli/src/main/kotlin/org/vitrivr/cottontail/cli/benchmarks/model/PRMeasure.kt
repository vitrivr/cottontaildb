package org.vitrivr.cottontail.cli.benchmarks.model

import org.vitrivr.cottontail.client.iterators.TupleIterator

/**
 * A Precision-Recal measurement that captures precision at different recall levels.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class PRMeasure(val precision: DoubleArray, val recall: DoubleArray) {

    companion object {
        /**
         * Obtains a P-R-graph for the given [groundtruth] and [testset] [List]s and the given number of recall levels.
         *
         * @param groundtruth The baseline to compare the data in the [testset] to.
         * @param testset The [testset] to obtain the P-R-graph for.
         * @param recallLevels The number of recall levels to consider.
         * @return [PRMeasure]
         */
        fun <T: Any> generate(groundtruth: List<T>, testset: List<T>, recallLevels: Int = 10): PRMeasure {
            require(testset.size == groundtruth.size) { "Cannot obtain P-R-data if test set is smaller than groundtruth."}
            val precision = DoubleArray(recallLevels)
            val recall = DoubleArray(recallLevels)
            val elementsPerLevel = Math.floorDiv(groundtruth.size, recallLevels)
            repeat(recallLevels) { level ->
                var relevantAndRetrieved = 0.0
                val sublist = groundtruth.subList(0, (level + 1) * elementsPerLevel)
                for (i in 0 until (level + 1) * elementsPerLevel) {
                    if (sublist.contains(testset[i])) relevantAndRetrieved += 1.0
                }
                precision[level] = relevantAndRetrieved / ((level + 1) * elementsPerLevel)
                recall[level] = relevantAndRetrieved / groundtruth.size
            }
            return PRMeasure(precision, recall)
        }

        /**
         * Obtains a P-R-graph for the given [groundtruth] [List] and [testset] [TupleIterator]s and the given number of recall levels.
         *
         * @param groundtruth The baseline to compare the data in the [testset] to.
         * @param testset The [testset] to obtain the P-R-graph for.
         * @param recallLevels The number of recall levels to consider.
         * @return [PRMeasure]
         */
        fun <T: Any> generate(groundtruth: List<T>, iterator: TupleIterator, columnIndex: Int = 0, recallLevels: Int = 10) : PRMeasure {
            val testset = ArrayList<T>(groundtruth.size)
            var i = 0
            while (iterator.hasNext() && i++ < groundtruth.size) {
                val next = iterator.next()
                testset.add(next[columnIndex] as T)
            }
            return generate(groundtruth, testset, recallLevels)
        }
    }

    init {
        require(precision.size == recall.size) { "Precision and recall array must be of same size." }
    }

    /**
     * Returns the number of recall levels for this [PRMeasure].
     *
     * @return Number of recall levels considered as [Int]
     */
    fun levels() = this.precision.size

    /**
     * Calculates and returns the average precision for this [PRMeasure].
     *
     * @return Average precision as [Double].
     */
    fun avgPrecision() = this.precision.sum() / this.precision.size

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is PRMeasure) return false

        if (!precision.contentEquals(other.precision)) return false
        if (!recall.contentEquals(other.recall)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = precision.contentHashCode()
        result = 31 * result + recall.contentHashCode()
        return result
    }
}