package org.vitrivr.cottontail.core.basics

import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.Tuple

/**
 * An objects that holds [Tuple] values and allows for deleting them based on their tuple ID.
 *
 * @see Tuple
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
interface Modifiable {
    /**
     * Inserts a [Tuple] into this [Modifiable].
     *
     * @return The generated [Tuple] of the new [Tuple].
     */
    fun insert(tuple: Tuple): Tuple

    /**
     * Updates a [Tuple] from this [Modifiable] based on its [TupleId].
     *
     * @param tuple The [Tuple] that should be updated
     */
    fun update(tuple: Tuple)

    /**
     * Deletes a [Tuple] from this [Modifiable] based on its [TupleId].
     *
     * @param tupleId The [TupleId] of the record that should be deleted
     */
    fun delete(tupleId: TupleId)
}