package org.vitrivr.cottontail.core.queries.planning.cost

import java.util.*
import kotlin.system.measureNanoTime

/**
 * The atomic [AtomicCostEstimator].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object AtomicCostEstimator {

    /** Number of repetitions to run when estimating atomic [Cost]s. */
    private const val ESTIMATION_REPETITION = 1_000_000

    /**
     * Estimates the cost of memory access  based on a series of measurements.
     */
    fun estimateAtomicMemoryAccessCost(): Float {
        val random = SplittableRandom()
        var time = 0L
        repeat(ESTIMATION_REPETITION) {
            var a = random.nextLong()
            var b = random.nextLong()
            var c = 0L
            time += measureNanoTime {
                c = a
                a = b
                b = c
            }
            c + b
        }
        return ((time) / (ESTIMATION_REPETITION * 3)) * 1e-9f
    }

    /**
     * Estimates the cost of a single floating point operation based on a series of measurements
     * for add, subtraction, multiplication and division
     */
    fun estimateAtomicFlopCost(): Float {
        val random = SplittableRandom()
        var timeAdd = 0L
        repeat(ESTIMATION_REPETITION) {
            val a = random.nextDouble()
            val b = random.nextDouble()
            timeAdd += measureNanoTime {
                a + b
            }
        }

        var timeSubtract = 0L
        repeat(ESTIMATION_REPETITION) {
            val a = random.nextDouble()
            val b = random.nextDouble()
            timeSubtract += measureNanoTime {
                a - b
            }
        }

        var timeMultiply = 0L
        repeat(ESTIMATION_REPETITION) {
            val a = random.nextDouble()
            val b = random.nextDouble()
            timeMultiply += measureNanoTime {
                a * b
            }
        }

        var timeDivide = 0L
        repeat(ESTIMATION_REPETITION) {
            val a = random.nextDouble()
            val b = random.nextDouble(1.0)
            timeDivide += measureNanoTime {
                a / b
            }
        }

        return ((timeAdd + timeSubtract + timeMultiply + timeDivide) / (ESTIMATION_REPETITION * 4)) * 1e-9f
    }
}