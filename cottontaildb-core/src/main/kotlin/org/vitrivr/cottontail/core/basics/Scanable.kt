package org.vitrivr.cottontail.core.basics

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.Tuple

/**
 * An objects that holds [Tuple] values and allows for scanning operation on those [Tuple] values.
 *
 * @see Tuple
 *
 * @author Ralph Gasser
 * @version 3.1.0
 */
interface Scanable {
    /**
     * Returns a [Cursor] for all the entries in this [Scanable].
     *
     * @param columns The [ColumnDef]s that should be scanned.
     * @param rename An array of [Name.ColumnName] that should be used instead of the actual [Name.ColumnName]. Empty by default.
     * @return [Cursor]
     */
    fun cursor(columns: Array<ColumnDef<*>>, rename: Array<Name.ColumnName> = emptyArray()): Cursor<Tuple>

    /**
     * Returns a [Cursor] for all the entries contained in the specified partition of [Scanable].
     *
     * @param columns The [ColumnDef]s that should be scanned.
     * @param partition The [LongRange] specifying the [TupleId]s that should be scanned.
     * @param rename An array of [Name.ColumnName] that should be used instead of the actual [Name.ColumnName]. Empty by default.
     * @return [Cursor]
     */
    fun cursor(columns: Array<ColumnDef<*>>, partition: LongRange, rename: Array<Name.ColumnName> = emptyArray()): Cursor<Tuple>
}