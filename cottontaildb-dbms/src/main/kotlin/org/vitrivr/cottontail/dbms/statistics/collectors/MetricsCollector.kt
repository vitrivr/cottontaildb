package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.database.ColumnDef
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

    companion object {
        /**
         * Function that, based on the [ColumnDef]'s [Types] returns the corresponding [MetricsCollector]
         *
         * @param type [Types] The [Types] for which to return the [MetricsCollector]
         * @param config The [MetricsConfig] to use for the [MetricsCollector]
         *
         * @return [MetricsCollector]
         */
        fun collectorForType(type: Types<*>, config: MetricsConfig) : MetricsCollector<*> = when (type) {
            Types.Boolean -> BooleanMetricsCollector(config)
            Types.Byte -> ByteMetricsCollector(config)
            Types.Short -> ShortMetricsCollector(config)
            Types.Date -> DateMetricsCollector(config)
            Types.Double -> DoubleMetricsCollector(config)
            Types.Float -> FloatMetricsCollector(config)
            Types.Int -> IntMetricsCollector(config)
            Types.Long -> LongMetricsCollector(config)
            Types.String -> StringMetricsCollector(config)
            Types.Uuid -> UuidMetricsCollector(config)
            Types.ByteString -> ByteStringMetricsCollector(config)
            Types.Complex32 -> Complex32MetricsCollector(config)
            Types.Complex64 -> Complex64MetricsCollector(config)
            is Types.BooleanVector -> BooleanVectorMetricsCollector(type.logicalSize, config)
            is Types.HalfVector -> HalfVectorMetricsCollector(type.logicalSize, config)
            is Types.FloatVector -> FloatVectorMetricsCollector(type.logicalSize, config)
            is Types.DoubleVector -> DoubleVectorMetricsCollector(type.logicalSize, config)
            is Types.ShortVector -> ShortVectorMetricsCollector(type.logicalSize, config)
            is Types.IntVector -> IntVectorMetricsCollector(type.logicalSize, config)
            is Types.LongVector -> LongVectorMetricsCollector(type.logicalSize, config)
            is Types.Complex32Vector -> Complex32VectorMetricsCollector(type.logicalSize, config)
            is Types.Complex64Vector -> Complex64VectorMetricsCollector(type.logicalSize, config)
        }
    }

    /** The [Types] of [Value] this [MetricsCollector] accepts. */
    val type: Types<T>

    /** The instance of [MetricsConfig] that is used to pass up variables to super classes */
    val config: MetricsConfig

    /** Number of distinct entries seen by this [MetricsCollector]. */
    val numberOfDistinctEntries : Long

    /** Number of null entries seen by this [MetricsCollector]. */
    val numberOfNonNullEntries : Long

    /** Number of non-null entries seen by this [MetricsCollector]. */
    val numberOfNullEntries : Long

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
    fun calculate(): ValueStatistics<T>
}