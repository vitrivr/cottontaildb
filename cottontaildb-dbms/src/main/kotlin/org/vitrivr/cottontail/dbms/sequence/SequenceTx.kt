package org.vitrivr.cottontail.dbms.sequence

import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.general.Tx

/**
 * A [Tx] that operates on a single [SequenceTx].
 *
 * [Tx]s are a unit of isolation for data  operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface SequenceTx: Tx {
    /**
     * Returns the next value of this [Sequence].
     *
     * @return The next [LongValue] value in the [Sequence].
     */
    fun next(): LongValue

    /**
     * Returns the current value of this [Sequence] without changing it.
     *
     * @return The next [LongValue] value in the [Sequence].
     */
    fun current(): LongValue

    /**
     * Resets this [Sequence], without changing it.
     *
     * @return The next [LongValue] value in the [Sequence].
     */
    fun reset(): LongValue
}