package org.vitrivr.cottontail.dbms.index.deg.deg


/**
 * Configuration for the [DynamicExplorationGraph].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class DEGConfig(val degree: Int, val kExt: Int,  val epsilonExt: Float)