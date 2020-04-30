package org.vitrivr.cottontail.model.basics

/**
 * An objects that holds [Record] values and allows for deleting them based on their tuple ID.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface Deletable {
    /**
     * Deletes a single [Record] from this [Deletable] based on its tuple ID.
     *
     * @param tupleId The tuple ID of the record that should be deleted
     * @return true if record was deleted, false otherwise.
     */
    fun delete(tupleId: Long)

    /**
     * Deletes a multiple [Record]s from this [Deletable] based on their tuple ID.
     *
     * @param tupleIds A collection tuple IDs of the records that should be deleted
     */
    fun deleteAll(tupleIds: Collection<Long>)
}