package ch.unibas.dmi.dbis.cottontail.model.basics

import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset

/**
 * An object that holds [Record] values and allows for scanning operation on those [Record] values in a parallel manner.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface ParallelScanable {
    /**
     * Applies the provided action to each [Record] in this [Recordset].
     *
     * @param parallelism The amount of parallelism. Defaults to 2.
     * @param action The action that should be applied.
     */
    fun forEach(parallelism: Short = 2, action: (Record) -> Unit)

    /**
     * Applies the provided mapping function to each [Record] in this [Filterable].
     *
     * * @param parallelism The amount of parallelism. Defaults to 2.
     * @param action The mapping function that should be applied.
     *
     * @return Collection of the results of the mapping function.
     */
    fun <R> map(parallelism: Short = 2, action: (Record) -> R): Collection<R>
}