package org.vitrivr.cottontail.execution.operators.basics

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator] that produces [Record]s either by mapping some input or by sourceing them.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class ProducingOperator(context: ExecutionEngine.ExecutionContext) : Operator(context) {

    /**
     * Produces the next [Record] and returns it. Can be null.
     */
    abstract fun next(): Record?
}