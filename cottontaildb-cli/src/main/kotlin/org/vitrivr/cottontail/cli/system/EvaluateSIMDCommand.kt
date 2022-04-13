package org.vitrivr.cottontail.cli.system

import org.apache.commons.math3.random.JDKRandomGenerator
import org.vitrivr.cottontail.cli.AbstractCottontailCommand
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator
import org.vitrivr.cottontail.core.values.types.Types
import kotlin.time.Duration
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * Command to start the Vector-API SPECIES-Evaluation for various dimensions.
 *
 * @author Colin Saladin
 * @version 1.0.0
 */
@ExperimentalTime
class EvaluateSIMDCommand : AbstractCottontailCommand.System(name = "simd", help = "Evaluate your system's SIMD performance for various dimensions with different SPECIES. Usage: system simd") {

    private val randomVector = JDKRandomGenerator(123456789)
    private val randomQuery = JDKRandomGenerator(987654321)
    private val numberOfNNSQueries = 100
    private val numberOfVectors = 500000
    private val toBeEvaluated = "Manhattan_Vectorized_PREFERRED"

    private val vectorList = mutableListOf<FloatVectorValue>()
    private val queryList = mutableListOf<FloatVectorValue>()

    private fun vectorInit(dimension: Int) {
        vectorList.clear()
        for (i in 0 until numberOfVectors) {
            vectorList.add(FloatVectorValueGenerator.random(dimension, randomVector))
        }
    }

    private fun queryInit(dimension: Int) {
        queryList.clear()
        for (i in 0 until numberOfNNSQueries) {
            queryList.add(FloatVectorValueGenerator.random(dimension, randomQuery))
        }
    }

    private fun warmUp() {

        vectorInit(2048)
        queryInit(2048)

        for (i in 0 until 5) {
            val distanceFunction = ManhattanDistance.FloatVectorVectorized(queryList[0].type as Types.FloatVector)
            distanceFunction(queryList[i], vectorList[i])
        }
    }

    override fun exec() {
        warmUp()

        for (i in 2 until 2048) {
            vectorInit(i)
            queryInit(i)

            var time: Duration

            //Print statements in .json-format
            val totalDimensionTime: Duration = measureTime {
                print("{\"Dimension\": $i, \"Version\": \"$toBeEvaluated\", ")

                val distanceFunction = ManhattanDistance.FloatVectorVectorized(queryList[0].type as Types.FloatVector)

                for (j in 0 until queryList.size) {
                    if (j != 0) {
                        print(", ")
                    }

                    time = measureTime {
                        vectorList.forEach { vector ->
                            distanceFunction(queryList[j], vector)
                        }
                    }

                    print("\"Time$j\": ${time.toDouble(DurationUnit.SECONDS)}")
                }

            }
            println(", \"TotalDimensionTime\": ${totalDimensionTime.toDouble(DurationUnit.SECONDS)}},")
        }
        println("]}")
    }
}
