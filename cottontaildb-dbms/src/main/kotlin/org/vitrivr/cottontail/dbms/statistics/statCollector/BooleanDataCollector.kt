package org.vitrivr.cottontail.dbms.statistics.statCollector

import org.vitrivr.cottontail.dbms.statistics.statData.DataMetrics

import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value


/**
 * A [DataMetrics] implementation for [BooleanValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class BooleanDataCollector: AbstractDataCollector<BooleanValue>(Types.Boolean) {

    /**
     * Receives the values for which to compute the statistics
     */
    override fun receive(value: Value?) {
        TODO("Not yet implemented")
        // TODO how to go from Value? to BooleanValue? here?
    }

    /**
     * Tells the collector to calculate the metrics which it does not do iteratively (e.g., mean etc.). Usually called after all elements were received
     */
    override fun calculate() {
        TODO("Not yet implemented")
    }

}