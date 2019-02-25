package ch.unibas.dmi.dbis.cottontail.database.index

import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Recordset

/**
 * A [Transaction] that operates on a single [Index]. [Transaction]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface IndexTransaction : Transaction {

    /**
     * (Re-)builds the underlying [Index] and can also be used to initialize it.
     *
     * @param columns List of columns to build the index. If null, the existing columns will be used.
     */
    fun update(columns: Array<ColumnDef<*>>? = null)

    /**
     * Performs a lookup through the underlying [Index] and returns a [Recordset].
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @return The resulting [Recordset].
     *
     * @throws DatabaseException.PredicateNotSupportedBxIndexException If predicate is not supported by [Index].
     */
    fun lookup(predicate: Predicate): Recordset

    /**
     * Performs a lookup through this [IndexTransaction] and returns a [Recordset].
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @param parallelism The amount of parallelism to allow for.
     * @return The resulting [Recordset].
     */
    fun lookupParallel(predicate: Predicate, parallelism: Short = 2): Recordset
}