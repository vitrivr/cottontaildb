package org.vitrivr.cottontail.utilities.math.ranking

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object RankingUtilities {
    /**
     * Calculates the recall at k for a given [List] of retrieved and relevant items. Comparison is based on object equality.
     *
     * @param retrieved [List] of retrieved items.
     * @param relevant [List] of relevant items.
     * @param k The number of items to consider.
     */
    fun <V> recallAtK(retrieved: List<V>, relevant: List<V>, k: Int): Float {
        require(k > 0) { "Parameter k must be greater than 0." }
        require(retrieved.size >= k) { "Number of retrieved items must be greater than k." }
        require(relevant.size >= k) { "Number of relevant items must be greater than k." }
        var score = 0.0f
        for (i in 0 until k) {
            if (relevant.contains(retrieved[i])) {
                score += 1.0f
            }
        }
        return score/k
    }

}