package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.metricsData.ByteValueMetrics
import org.vitrivr.cottontail.dbms.statistics.metricsData.IntValueMetrics
import java.lang.Integer.max
import java.lang.Integer.min

/**
 * A [MetricsCollector] implementation for [IntValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class IntMetricsCollector : RealMetricsCollector<IntValue>(Types.Int) {

    /** Local Metrics */
    var min : Int = 0
    var max : Int = 0
    var sum : Double = 0.0


    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        super.receive(value)
        if (value != null && value is IntValue) {
            // set new min, max, and sum
            min = min(value.value, min)
            max = max(value.value, max)
            sum += value.value
        }
    }

    override fun calculate(): IntValueMetrics {
        return  IntValueMetrics(
            numberOfNullEntries,
            numberOfNonNullEntries,
            numberOfDistinctEntries,
            IntValue(min),
            IntValue(max),
            DoubleValue(sum)
        )
    }


}
