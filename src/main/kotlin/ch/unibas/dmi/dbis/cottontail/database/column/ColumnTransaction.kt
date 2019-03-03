package ch.unibas.dmi.dbis.cottontail.database.column

import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.model.basics.*
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.values.Value

/**
 * A [Transaction] that operates on a single [Column]. [Transaction]s are a unit of isolation for data
 * operations (read/write). This interface defines the basic operations supported by such a [Transaction].
 * However, it does not dictate the isolation level. It is up to the implementation to define and implement
 * the desired level of isolation.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface ColumnTransaction<T: Any> : Transaction, Countable, Scanable, ParallelScanable, Filterable {
    /**
     * Gets and returns an entry from this [Column].
     *
     * @param tupleId The ID of the desired entry
     * @return The desired entry.
     *
     * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
     */
    fun read(tupleId: Long): Value<T>?

    /**
     * Gets and returns several entries from this [Column].
     *
     * @param tupleIds The IDs of the desired entries
     * @return List of the desired entries.
     *
     * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
     */
    fun readAll(tupleIds: Collection<Long>): Collection<Value<T>?>

    /**
     * Inserts a new record in this [Column].
     *
     * @param record The record that should be inserted. Can be null!
     * @return The tupleId of the inserted record OR the allocated space in case of a null value.
     */
    fun insert(record: Value<T>?): Long

    /**
     * Inserts a list of new records in this [Column].
     *
     * @param records The records that should be inserted. Can contain null values!
     * @return The tupleId of the inserted record OR the allocated space in case of a null value.
     */
    fun insertAll(records: Collection<Value<T>?>): Collection<Long>

    /**
     * Updates the entry with the specified tuple ID and sets it to the new value.
     *
     * @param tupleId The ID of the record that should be updated
     * @param value The new value.
     */
    fun update(tupleId: Long, value: Value<T>?)

    /**
     * Updates the entry with the specified tuple ID and sets it to the new value.
     *
     * @param tupleId The ID of the record that should be updated
     * @param value The new value.
     * @param expected The value expected to be there.
     */
    fun compareAndUpdate(tupleId: Long, value: Value<T>?, expected: Value<T>?): Boolean

    /**
     * Deletes a record from this [Column].
     *
     * @param tupleId The ID of the record that should be deleted
     */
    fun delete(tupleId: Long)

    /**
     * Deletes all the specified records from this [Column].
     *
     * @param tupleIds The IDs of the records that should be deleted.
     */
    fun deleteAll(tupleIds: Collection<Long>)
}