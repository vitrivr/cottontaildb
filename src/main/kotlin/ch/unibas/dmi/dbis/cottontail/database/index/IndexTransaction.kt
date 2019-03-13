package ch.unibas.dmi.dbis.cottontail.database.index

import ch.unibas.dmi.dbis.cottontail.database.general.Transaction
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Filterable
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset

/**
 * A [Transaction] that operates on a single [Index]. [Transaction]s are a unit of isolation for data operations (read/write).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal interface IndexTransaction : Transaction, Filterable {

    /**
     * (Re-)builds the underlying [Index].
     */
    fun rebuild()

    /**
     * Checks if this [IndexTransaction] can process the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    fun canProcess(predicate: Predicate): Boolean

    /**
     * Performs a lookup through this [IndexTransaction] and returns a [Recordset].
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @param parallelism The amount of parallelism to allow for.
     * @return The resulting [Recordset].
     */
    fun lookupParallel(predicate: Predicate, parallelism: Short = 2): Recordset
}