package org.vitrivr.cottontail.core.basics

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.Tuple

/**
 * An objects that holds [Tuple] values and allows for scanning operation on those [Tuple] values.
 *
 * @see Tuple
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
    fun cursor(columns: Array<ColumnDef<*>>): Cursor<Tuple>

    /**
     * Returns a [Cursor] for all the entries contained in the specified partition of [Scanable].
     *
     * @param columns The [ColumnDef]s that should be scanned.
     * @param partition The [LongRange] specifying the [TupleId]s that should be scanned.
     * @return [Cursor]
     */
    fun cursor(columns: Array<ColumnDef<*>>, partition: LongRange): Cursor<Tuple>
}