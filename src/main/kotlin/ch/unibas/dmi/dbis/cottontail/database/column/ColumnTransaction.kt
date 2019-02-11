package ch.unibas.dmi.dbis.cottontail.database.column

import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.model.basics.Tuple
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException

/**
 * A [Transaction] that operates on a single [Column]. [Transaction]s are a unit of isolation for data
 * operations (read/write). This interface defines the basic operations supported by such a [Transaction].
 * However, it does not dictate the isolation level. It is up to the implementation to define and implement
 * the desired level of isolation.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface ColumnTransaction<T: Any> : Transaction {
    /**
     * Gets and returns an entry from this [Column].
     *
     * @param tupleId The ID of the desired entry
     * @return The desired entry.
     *
     * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
     */
    fun read(tupleId: Long): T?

    /**
     * Gets and returns several entries from this [Column].
     *
     * @param tupleIds The IDs of the desired entries
     * @return List of the desired entries.
     *
     * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
     */
    fun readAll(tupleIds: Collection<Long>): Collection<T?>

    /**
     * Returns the number of entries in this [Column]. Action acquires a global read dataLock for the [Column].
     *
     * @return The number of entries in this [Column].
     */
    fun count(): Long

    /**
     * Applies the provided mapping function on each value found in this [Column], returning a collection
     * of the desired output values.
     *
     * @param action The tasks that should be applied.
     * @return A collection of Pairs mapping the tupleId to the generated value.
     */
    fun <R> map(action: (T?) -> R?): Collection<Tuple<R?>>

    /**
     * Applies the provided predicate function on each value found in this [Column], returning a collection
     * of output values that pass the predicate's test (i.e. return true)
     *
     * @param predicate The tasks that should be applied.
     * @return A filtered collection [Column] values that passed the test.
     */
    fun filter(predicate: (T?) -> Boolean): Collection<Tuple<T?>>

    /**
     * Applies the provided function on each element found in this [Column]. The provided function cannot not change
     * the data stored in the [Column]!
     *
     * @param action The function to apply to each [Column] entry.
     */
    fun forEach(action: (Long,T?) -> Unit)

    /**
     * Applies the provided function on each element found in this [Column]. The provided function cannot not change
     * the data stored in the [Column]!
     *
     * @param action The function to apply to each [Column] entry.
     * @param parallelism The desired amount of parallelism (i.e. the number of co-routines to spawn).
     */
    fun parallelForEach(action: (Long,T?) -> Unit, parallelism: Short = 2)

    /**
     * Inserts a new record in this [Column].
     *
     * @param record The record that should be inserted. Can be null!
     * @return The tupleId of the inserted record OR the allocated space in case of a null value.
     */
    fun insert(record: T?): Long

    /**
     * Inserts a list of new records in this [Column].
     *
     * @param records The records that should be inserted. Can contain null values!
     * @return The tupleId of the inserted record OR the allocated space in case of a null value.
     */
    fun insertAll(records: Collection<T?>): Collection<Long>

    /**
     * Updates the entry with the specified tuple ID and sets it to the new value.
     *
     * @param tupleId The ID of the record that should be updated
     * @param value The new value.
     */
    fun update(tupleId: Long, value: T?)

    /**
     * Updates the entry with the specified tuple ID and sets it to the new value.
     *
     * @param tupleId The ID of the record that should be updated
     * @param value The new value.
     * @param expected The value expected to be there.
     */
    fun compareAndUpdate(tupleId: Long, value: T?, expected: T?): Boolean

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