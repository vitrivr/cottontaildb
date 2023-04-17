package org.vitrivr.cottontail.core.basics

import org.vitrivr.cottontail.core.database.TupleId

/**
 * An objects that holds [Record] values and allows for deleting them based on their tuple ID.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
interface Modifiable {
    /**
     * Inserts a [Record] into this [Modifiable].
     *
     * @return The generated [Record] of the new [Record].
     */
    fun insert(record: Record): Record

    /**
     * Updates a [Record] from this [Modifiable] based on its [TupleId].
     *
     * @param record The [Record] that should be updated
     */
    fun update(record: Record)

    /**
     * Deletes a [Record] from this [Modifiable] based on its [TupleId].
     *
     * @param tupleId The [TupleId] of the record that should be deleted
     */
    fun delete(tupleId: TupleId)
}