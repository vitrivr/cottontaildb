package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import java.util.random.RandomGenerator

/**
 * Config for Cottontail DB's management of statistics.
 *
 * @author Florian Burkhardt
 * @version 1.0.1
 */
@Serializable
data class StatisticsConfig(
    /** Defines after how many changes (percentage of total population) statistics must be re-generated. Should be between 0 (exclusive) and 1 (inclusive). */
    val threshold: Float = 0.1f, // default is 10%

    /**
     * Probability used for data sampling. The sample size considered by the statistics manager is roughly the sampleProbabilit * populationSize.
     *
     * Should be between 0 (exclusive) and 1 (inclusive). Default to 0.1f (10%)
     */
    val sampleProbability: Float = 0.1f, // default is 10%

    /** The minimum sample size. The statistics manager makes sure, that at least this many sample are gathered, regardless of the [sampleProbability]. */
    val minimumSampleSize: Long = 100_000L,

    /**
     * Defines the false positive probability used by the BloomFilter when detecting the numberOfDistinctElements.
     *
     * The false positive probability is represented by a float value between 0 and 1, and it determines the desired maximum probability of false positives that the filter will generate.
     * The lower the false positive probability, the larger the size of the Bloom filter and the more expensive it is to use and maintain.
     */
    val falsePositiveProbability: Double = 0.01, // Default is 1%. Typical value between 1% to 10%

    /** The random number generator to use. */
    val randomGeneratorName: String = "L32X64MixRandom",
) {
    init {
        require(this.threshold in 0.0f .. 1.0f) { "The threshold must lie between 0.0 and 1.0 but is ${this.threshold}."}
        require(this.sampleProbability in 0.0f .. 1.0f) { "The probability must lie between 0.0 and 1.0 but is ${this.sampleProbability}."}
        require(this.falsePositiveProbability in 0.0f .. 1.0f) { "The falsePositiveProbability must lie between 0.0 and 1.0 but is ${this.falsePositiveProbability}."}
    }

    /**
     * Creates and returns a new [RandomGenerator] instance.
     *
     * @return [RandomGenerator]
     */
    fun randomGenerator(): RandomGenerator = RandomGenerator.of(this.randomGeneratorName)
}




