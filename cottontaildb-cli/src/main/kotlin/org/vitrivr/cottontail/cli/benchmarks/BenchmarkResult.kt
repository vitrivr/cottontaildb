package org.vitrivr.cottontail.cli.benchmarks

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
    val rankedAccuracy: Double? = null,
    val overlapAccuracy: Double? = null
) {

    companion object {
        /** The header line in a CSV file. */
        const val CSV_HEADER = "name,description,phase,repetition,start_ms,end_ms,duration_ms,ranked_accuracy,overlap_accuracy"
    }

    /**
     * Converts this [BenchmarkResult] to a line in a CSV file.
     */
    fun toCSVLine(): String = "$name,${description ?: ""},$phase,$repetition,$startMs,$endMs,$durationMs,${rankedAccuracy ?: ""},${overlapAccuracy ?: ""}"
}