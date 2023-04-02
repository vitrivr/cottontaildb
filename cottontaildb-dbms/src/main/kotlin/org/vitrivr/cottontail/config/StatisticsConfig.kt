package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable

/**
 * Config for Cottontail DB's management of statistics.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */

@Serializable
data class StatisticsConfig(
    /** Threshold for the Statistics Manager. Defines after how many changes (percentage of changes) a new statistic is generated. Should be between 0 (exclusive) and 1 (inclusive). */
    var threshold: Float = 0.1F, // default is 10%

    /** Defines the size of the sample from which the database statistics will be computed. E.g., when set to 10% the StatisticsManager will randomly sample 10% entities from all entities and compute new statistics based on them. Should be between 0 (exclusive) and 1 (inclusive).  */
    var probability: Float = 0.1F, // default is 10%

    /** Defines the false positive probability used by the BloomFilter when detecting the numberOfDistinctElements.
     * The false positive probability is represented by a float value between 0 and 1, and it determines the desired maximum probability of false positives that the filter will generate.
     * The lower the false positive probability, the larger the size of the Bloom filter and the more expensive it is to use and maintain.*/
    var falsePositiveProbability: Float = 0.01F // Default is 1%. Typical value between 1% to 10%

    // TODO maybe add a threshold for the numberOfEntries there have to be before sampling?
    // Todo check if require arguments (below) could somehow work?
) {
    // custom setter for threshold to check for input
    /*var threshold: Float
        get() = _threshold
        set(value) {
            require(value > 0.0F && value <= 1.0F) { "Threshold must be between 0 (exclusive) and 1 (inclusive)" }
            _threshold = value
        }
    var probability: Float
        get() = _probability
        set(value) {
            require(value > 0.0F && value <= 1.0F) { "Probability must be between 0 (exclusive) and 1 (inclusive)" }
            _probability = value
        }*/
}