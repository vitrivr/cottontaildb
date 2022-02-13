package org.vitrivr.cottontail.core.basics

import org.vitrivr.cottontail.core.database.ColumnDef

/**
 * An objects that holds [Record] values and allows for scanning operation on those [Record] values.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
interface Scanable {
    /**
     * Returns a [Cursor] for all the entries in this [Scanable].
     *
     * @param columns The [ColumnDef]s that should be scanned.
     * @return [Cursor]
     */
    fun cursor(columns: Array<ColumnDef<*>>): Cursor<Record>

    /**
     * Returns a [Cursor] for all the entries contained in the specified partition of [Scanable].
     *
     * @param columns The [ColumnDef]s that should be scanned.
     * @param partitionIndex The [partitionIndex] for this [cursor] call.
     * @param partitions The total number of partitions for this [cursor] call.
     * @return [Cursor]
     */
    fun cursor(columns: Array<ColumnDef<*>>, partitionIndex: Int, partitions: Int): Cursor<Record>
}