package org.vitrivr.cottontail.dbms.index.basics.avc

import org.vitrivr.cottontail.core.database.TupleId
import java.util.*

/**
 * Auxiliary value collections are data structures used by [AbstractHDIndex] structures to keep track of changes
 * (i.e., INSERT, UPDATEs and DELETEs)  that could not be applied to the [AbstractHDIndex] directly, because of
 * write-model limitations.
 *
 * The [AuxiliaryValueCollection] can be used by the [AbstractHDIndex] to mitigate the effect of such changes,
 * which comes at an additional cost.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface AuxiliaryValueCollection {
    /** A [SortedSet] of all [TupleId]s that have been inserted. Order of INSERTS matter because of partitioned scans! */
    val insertSet: SortedSet<TupleId>

    /** A [Set] of all [TupleId]s that have been updated. */
    val updateSet: Set<TupleId>

    /** A [Set] of all [TupleId]s that have been deleted. */
    val deleteSet: Set<TupleId>

    /**
     * Applies an INSERT operation for the given [TupleId] and [Value].
     *
     * @param tupleId The [TupleId] that has been inserted.
     */
    fun applyInsert(tupleId: TupleId)

    /**
     * Applies an UPDATE operation for the given [TupleId] and [Value].
     *
     * @param tupleId The [TupleId] that has been updated.
     */
    fun applyUpdate(tupleId: TupleId)

    /**
     * Applies an DELETE operation for the given [TupleId] and [Value].
     *
     * @param tupleId The [TupleId] that has been deleted.
     */
    fun applyDelete(tupleId: TupleId)
}