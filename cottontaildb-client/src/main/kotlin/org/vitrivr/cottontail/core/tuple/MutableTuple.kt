package org.vitrivr.cottontail.core.tuple

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Value

/**
 * A mutable version of a [Tuple] as returned and processed by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface MutableTuple: Tuple {
    /**
     * Sets the [Value] for the specified column index in this [Tuple].
     *
     * @param index The column index for which to set the value.
     * @param value The new [Value]
     */
    operator fun set(index: Int, value: Value?)

    /**
     * Sets the [Value]  for the specified [ColumnDef] in this [Tuple].
     *
     * @param column The [ColumnDef] for which to set the value.
     * @param value The new [Value]
     */
    operator fun set(column: ColumnDef<*>, value: Value?) = this.set(this.indexOf(column), value)

    /**
     * Sets the [Value] for the specified [Name.ColumnName] in this [Tuple].
     *
     * @param column The [Name.ColumnName] for which to set the value.
     * @param value The new [Value]
     */
    operator fun set(column: Name.ColumnName, value: Value?) = this.set(this.indexOf(column), value)

    /**
     * Sets the [Value] for the specified column name in this [Tuple].
     *
     * @param column The colum name for which to set the value.
     * @param value The new [Value]
     */
    operator fun set(column: String, value: Value?) = this.set(this.indexOf(Name.ColumnName.create(column)), value)
}