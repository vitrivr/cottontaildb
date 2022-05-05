package org.vitrivr.cottontail.cli.basics

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import org.vitrivr.cottontail.cli.benchmarks.BenchmarkResult
import org.vitrivr.cottontail.client.SimpleClient
import java.io.BufferedWriter
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption

/**
 * An [AbstractCottontailCommand] designed to record and execute benchmarks.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractBenchmarkCommand(protected val client: SimpleClient, name: String, help: String, expand: Boolean = true): AbstractCottontailCommand(name, help, expand) {

    /** Output path for benchmark results. */
    protected val out: BufferedWriter? by option(
        "-o",
        "--out",
        help = "If set, query will be exported into files instead of CLI."
    ).convert {
        val path = Paths.get(it)
        val out = Files.newBufferedWriter(path.resolve("results-${System.currentTimeMillis()}.csv"), StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND)
        out.write(BenchmarkResult.CSV_HEADER)
        out
    }

    /** The number of repetitions to perform. */
    protected val warmup: Int  by option(
        "-w",
        "--warmup",
        help = "The number of warmup rounds to perform before starting the benchmark."
    ).convert { it.toInt() }.default(1)

    /** The number of repetitions to perform when executing the benchmark. */
    protected val repeat: Int by option(
        "-r",
        "--repeat",
        help = "Number of repetitions to perform while benchmarking."
    ).convert { it.toInt() }.default(1)

    /** The name of this [AbstractBenchmarkCommand]. */
    abstract val name: String

    /** The number of phases that this [AbstractBenchmarkCommand] exhibits. Defaults to 1. */
    protected open val phases: Int = 1

    /** Performs preparation operations before running this [AbstractBenchmarkCommand]. */
    abstract fun prepare(phase: Int)


    /** Performs preparation operations after running this [AbstractBenchmarkCommand]. */
    abstract fun cleanup(phase: Int)

    /** Performs warmup phase for this [AbstractBenchmarkCommand]. */
    abstract fun warmup(phase: Int, repetition: Int)

    /**
     * Performs the actual benchmark phase for this [AbstractBenchmarkCommand].
     *
     * @return [BenchmarkResult]
     */
    abstract fun workload(phase: Int, repetition: Int): BenchmarkResult

    /**
     * Makes sure that the output stream is always closed.
     */
    override fun exec() {
        try {
            repeat(this.phases) {
                val phase = it + 1
                var cleanup = false
                try {
                    println("Starting ${this.name} phase $phase out of ${this.phases}.")
                    println("${this.name} ($phase/${this.phases}): Preparing benchmark...")
                    this.prepare(phase)
                    cleanup = true

                    /* Performs benchmark warmup. */
                    repeat(this.warmup) { w ->
                        val warmup = w + 1
                        println("${this.name} ($phase/${this.phases}): Executing warmup workload (${warmup}/${this.warmup})...")
                        this.warmup(phase, warmup)
                    }

                    /* Performs actual benchmark. */
                    repeat(this.repeat) { r ->
                        val repeat = r + 1
                        print("${this.name} ($phase/${this.phases}): Executing benchmark workload (${repeat}/${this.repeat})...")
                        val result = this.workload(phase, repeat)
                        println(", took: ${result.durationMs}ms")
                        this.out?.write(result.toCSVLine())
                    }

                    /* Performs the cleanup for the current phase. */
                    println("${this.name} ($phase/${this.phases}): Cleaning-up benchmark...")
                    this.cleanup(phase)
                    cleanup = false
                    println("${this.name} $phase completed.")
                } catch (e: Throwable) {
                    System.err.println("Benchmark phase $phase failed: ${e.message}")
                    if (cleanup) this.cleanup(phase)
                    return
                }
            }
        } finally {
            this.out?.close()
        }
    }

    /**
     * Prints the result depending on how the command was initialized to the console AND/OR the output file.
     *
     * @param result The [BenchmarkResult] to obtain.
     */
    protected fun out(result: BenchmarkResult) {

        println(result.toString())
    }
}