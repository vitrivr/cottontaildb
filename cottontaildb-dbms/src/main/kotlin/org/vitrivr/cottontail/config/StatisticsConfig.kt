package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import java.util.random.RandomGenerator

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
    var falsePositiveProbability: Double = 0.01, // Default is 1%. Typical value between 1% to 10%

    /** The random number generator to use. */
    private var randomGeneratorName: String = "L32X64MixRandom",

    /** The seed with which to use the randomNumberGenerator */
    private var randomSeed: Int? = null,

    /** The Random Number Generator we use. Set to default */
    var randomGenerator: RandomGenerator = RandomGenerator.getDefault()

    // TODO maybe add a threshold for the numberOfEntries there have to be before sampling?
)
{
    init {
        require(this.threshold in 0.0f .. 1.0f) { "The threshold must lie between 0.0 and 1.0 but is ${this.threshold}."}
        require(this.probability in 0.0f .. 1.0f) { "The probability must lie between 0.0 and 1.0 but is ${this.probability}."}
        require(this.falsePositiveProbability in 0.0 .. 1.0) { "The falsePositiveProbability must lie between 0.0 and 1.0 but is ${this.falsePositiveProbability}."}
        require(this.randomGeneratorName in setOf("Xoroshiro128PlusPlus", "Xoshiro256PlusPlus", "L128X1024MixRandom", "128X128MixRandom", "L128X256MixRandom", "L32X64MixRandom", "L64X1024MixRandom", "L64X128MixRandom", "L64X128StarStarRandom", "L64X256MixRandom")) { "randomGeneratorName must be one of: Xoroshiro128PlusPlus, Xoshiro256PlusPlus, L128X1024MixRandom, 128X128MixRandom, L128X256MixRandom, L32X64MixRandom, L64X1024MixRandom, L64X128MixRandom, L64X128StarStarRandom, L64X256MixRandom, but is ${this.randomGeneratorName}" }
        this.randomGenerator = RandomGenerator.of(this.randomGeneratorName) // Init the randomGenerator based on the name
    }
}




