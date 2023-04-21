package org.vitrivr.cottontail.cli.system

import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import org.apache.commons.math3.random.JDKRandomGenerator
import org.vitrivr.cottontail.cli.basics.AbstractCottontailCommand
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator
import org.vitrivr.cottontail.core.values.types.Types
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * Command to start the Vector-API SPECIES-Evaluation for various dimensions.
 *
 * @author Colin Saladin & Ralph Gasser
 * @version 1.0.0
 */
@ExperimentalTime
class FindVectorisationThresholdCommand : AbstractCottontailCommand(name = "simd-probe", help = "Evaluate your system's SIMD performance for various dimensions. Usage: system simd-probe", false) {


    /** The [JDKRandomGenerator] used by this [FindVectorisationThresholdCommand]. */
    private val random = JDKRandomGenerator(System.currentTimeMillis().toInt())

    /** Flag that can be used to directly provide confirmation. */
    private val collectionSize: Int by option("-c", "--count", help = "Size of the in-memory test collection.").int().default(500000)

    /**
     * Generates a new collection of random probing vectors.
     *
     * @param dimension The dimensionality to create test collection for.
     */
    private fun newCollection(dimension: Int) = (0 until this.collectionSize).map {
        FloatVectorValueGenerator.random(dimension, this.random)
    }

    /**
     * Generates a new, andom query vector.
     *
     * @param dimension The dimensionality to create query vector for.
     */
    private fun newQuery(dimension: Int) = FloatVectorValueGenerator.random(dimension, this.random)

    /**
     * Executes this command.
     */
    override fun exec() {
        var dimension = 0
        var total = Duration.ZERO
        var totalVectorised = Duration.ZERO
        var passedThresholdCounter = 0
        for (d in 64 until 2048) {
            val query = this.newQuery(d)
            val collection = this.newCollection(d)
            val distance = EuclideanDistance.FloatVector(Types.FloatVector(d))
            val vectorised = distance.vectorized()
            total = Duration.ZERO
            totalVectorised = Duration.ZERO
            for (p in collection) {
                val measured = measureTimedValue { distance(query, p) }
                val measuredVectorised = measureTimedValue { vectorised(query, p) }
                total += measured.duration
                totalVectorised += measuredVectorised.duration
            }
            if (totalVectorised < (total * 0.9)) {
                dimension = d
                passedThresholdCounter += 1
            } else {
                passedThresholdCounter = 0
            }
            if (passedThresholdCounter > 10) {
                break
            }
            print("Probing vector dimensions to find SIMD threshold: d=$d (ts = $total, tv = $totalVectorised)\r")
        }
        println("Identified d=$dimension as a good threshold for vectorisation (ts = $total, tv = $totalVectorised)")
    }
}
