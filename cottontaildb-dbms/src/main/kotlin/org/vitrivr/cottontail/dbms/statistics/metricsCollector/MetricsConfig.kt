package org.vitrivr.cottontail.dbms.statistics.metricsCollector

import org.vitrivr.cottontail.config.StatisticsConfig

/**
 * This is an object that contains all the values that are passed up from the collectors to the super classes.
 * This is done so that when additional values are desired we can add these here directly instead of passing an additional value in all 10+ collectors.
 *  @author Florian Burkhardt
 *  @version 1.0.0
 */
data class MetricsConfig(
    val statisticsConfig: StatisticsConfig,
    val expectedNumElements: Int
)
