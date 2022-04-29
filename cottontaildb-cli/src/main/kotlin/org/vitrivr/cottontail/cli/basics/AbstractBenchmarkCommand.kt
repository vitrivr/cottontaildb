package org.vitrivr.cottontail.cli.basics

import com.github.ajalt.clikt.parameters.options.convert
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import org.vitrivr.cottontail.client.SimpleClient
import java.nio.file.Path
import java.nio.file.Paths

/**
 *
 */
abstract class AbstractBenchmarkCommand(protected val client: SimpleClient, name: String, help: String, expand: Boolean = true): AbstractCottontailCommand(name, help, expand) {

    /** Output path for benchmark results. */
    protected val out: Path? by option(
        "-o",
        "--out",
        help = "If set, query will be exported into files instead of CLI."
    ).convert { Paths.get(it) }

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
    ).convert { it.toInt() }.default(10)
}