package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics

/**
 * A [MetricsCollector] can be used to collection statistics and metrics about a collection of [Value]s (typically stored in the same column).
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface MetricsCollector <T : Value> {
    /** The [Types] of [Value] this [MetricsCollector] accepts. */
    val type: Types<T>

    /** The instance of [MetricsConfig] that is used to pass up variables to super classes */
    val config: MetricsConfig

    /** Number of distinct entries seen by this [MetricsCollector]. */
    var numberOfDistinctEntries : Long

    /** Number of null entries seen by this [MetricsCollector]. */
    var numberOfNonNullEntries : Long

    /** Number of non-null entries seen by this [MetricsCollector]. */
    var numberOfNullEntries : Long

    /**
     * Receives a [Value] that should be considered for analysis.
     *
     * @param value The [Value] received.
     */
    fun receive(value: T?)

    /**
     * Generates and returns the [ValueStatistics] based on the current state of the [MetricsCollector].
     *
     * @return Generated [ValueStatistics]
     */
    fun calculate(probability: Float): ValueStatistics<T>
}