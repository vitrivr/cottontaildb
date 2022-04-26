package org.vitrivr.cottontail.dbms.execution

/**
 * An [ExecutionContext] can be used to query the state of the execution environment.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
interface ExecutionContext {
    /** The number of available workers for query execution. */
    val availableQueryWorkers: Int

    /** The number of available workers for intra-query parallelisation. */
    val availableIntraQueryWorkers: Int
}