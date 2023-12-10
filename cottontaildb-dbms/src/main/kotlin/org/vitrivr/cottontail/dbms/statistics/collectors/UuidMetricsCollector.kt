package org.vitrivr.cottontail.dbms.statistics.collectors

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.UuidValue
import org.vitrivr.cottontail.dbms.statistics.values.UuidValueStatistics

/**
 * A specialized [MetricsCollector] implementation for [UuidValue]s.
 *
 * @author Ralph Gasser
 * @author Florian Burkhardt
 * @version 1.0.0
 */
class UuidMetricsCollector(config: MetricsConfig) : AbstractScalarMetricsCollector<UuidValue>(Types.Uuid, config) {
    override fun calculate(probability: Float): UuidValueStatistics {
        val sampleMetrics = UuidValueStatistics(this.numberOfNullEntries, this.numberOfNonNullEntries, this.numberOfDistinctEntries)
        return  UuidValueStatistics(1/probability, sampleMetrics)
    }
}