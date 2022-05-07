package org.vitrivr.cottontail.cli.benchmarks.model

/**
 * A [Benchmark] as executed by an [AbstractBenchmarkCommand].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class Benchmark(val name: String, val phases: Int = 1) {
    /** Performs preparation operations before running this [Benchmark]. */
    abstract fun prepare(phase: Int)

    /** Performs preparation operations after running this [Benchmark]. */
    abstract fun cleanup(phase: Int)

    /** Performs warmup phase for this [Benchmark]. */
    abstract fun warmup(phase: Int, repetition: Int)

    /**
     * Performs the actual benchmark phase for this [Benchmark].
     *
     * @return [BenchmarkResult]
     */
    abstract fun workload(phase: Int, repetition: Int): BenchmarkResult
}