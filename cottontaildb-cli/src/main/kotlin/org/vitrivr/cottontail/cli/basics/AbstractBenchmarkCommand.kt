package org.vitrivr.cottontail.cli.basics

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import org.vitrivr.cottontail.cli.benchmarks.model.Benchmark
import org.vitrivr.cottontail.cli.benchmarks.model.BenchmarkResult
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
        out.newLine()
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
    ).convert { it.toInt() }.default(3)

    /**
     * Generates a new [Benchmark] object for this [AbstractBenchmarkCommand].
     *
     * @return [Benchmark]
     */
    abstract fun initialize(): Benchmark

    /**
     * Makes sure that the output stream is always closed.
     */
    override fun exec() {
        /* Create new benchmark. */
        try {
            val benchmark = this.initialize()
            repeat(benchmark.phases) {
                val phase = it + 1
                var cleanup = false
                try {
                    println("Starting ${benchmark.name} phase $phase out of ${benchmark.phases}.")
                    println("${benchmark.name} ($phase/${benchmark.phases}): Preparing benchmark...")
                    benchmark.prepare(phase)
                    cleanup = true

                    /* Performs benchmark warmup. */
                    repeat(this.warmup) { w ->
                        val warmup = w + 1
                        println("${benchmark.name} ($phase/${benchmark.phases}): Executing warmup workload (${warmup}/${this.warmup})...")
                        benchmark.warmup(phase, warmup)
                    }

                    /* Performs actual benchmark. */
                    repeat(this.repeat) { r ->
                        val repeat = r + 1
                        print("${benchmark.name} ($phase/${benchmark.phases}): Executing benchmark workload (${repeat}/${this.repeat})...")
                        val result = benchmark.workload(phase, repeat)
                        println(", took: ${result.durationMs}ms")
                        this.out?.write(result.toCSVLine())
                        this.out?.newLine()
                    }

                    /* Performs the cleanup for the current phase. */
                    println("${benchmark.name} ($phase/${benchmark.phases}): Cleaning-up benchmark...")
                    benchmark.cleanup(phase)
                    cleanup = false
                    println("${benchmark.name} $phase completed.")
                } catch (e: Throwable) {
                    System.err.println("Benchmark phase $phase failed: ${e.message}")
                    if (cleanup) benchmark.cleanup(phase)
                    return
                }
            }
        } finally {
            this.out?.close()
        }
    }
}