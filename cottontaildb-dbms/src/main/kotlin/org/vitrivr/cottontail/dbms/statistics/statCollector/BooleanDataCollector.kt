package org.vitrivr.cottontail.dbms.statistics.statCollector

import org.vitrivr.cottontail.dbms.statistics.statData.DataMetrics

import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.types.Types


/**
 * A [DataMetrics] implementation for [BooleanValue]s.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
class BooleanDataCollector: AbstractDataCollector<BooleanValue>(Types.Boolean) {

    /**
     * Collects the data necessary to estimate metrics for the corresponding  [DataMetrics].
     *
     * @return Unit
     */
    override fun collector() {
        //super.collector()
        TODO("implement")
    }
}