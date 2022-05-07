package org.vitrivr.cottontail.cli.benchmarks.model

/**
 * A result of a single benchmark iteration as produced by a [AbstractBenchmarkCommand]
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class BenchmarkResult(
    val name: String,
    val description: String? = null,
    val phase: Int,
    val repetition: Int,
    val startMs: Long,
    val endMs: Long,
    val durationMs: Long,
    val prgraph: PRMeasure? = null
) {

    companion object {
        /** The header line in a CSV file. */
        const val CSV_HEADER = "name,description,phase,repetition,start_ms,end_ms,duration_ms,ap"
    }

    /**
     * Converts this [BenchmarkResult] to a line in a CSV file.
     */
    fun toCSVLine(): String = "\"$name\",\"${description ?: ""}\",$phase,$repetition,$startMs,$endMs,$durationMs,${prgraph?.avgPrecision() ?: ""}"
}