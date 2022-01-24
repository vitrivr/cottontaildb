package org.vitrivr.cottontail.database.queries.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.Types

/**
 * Enumeration of all [Projection] operations supported by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class Projection(val aggregating: Boolean) {
    SELECT(false),
    SELECT_DISTINCT(false),
    COUNT(true),
    COUNT_DISTINCT(true),
    EXISTS(true),
    SUM(true),
    MAX(true),
    MIN(true),
    MEAN(true);


    /**
     * Converts this [Projection] to a [String] labels.
     *
     * @return String label.
     */
    fun label() = this.toString().lowercase().replace("_", "").uppercase()

    /**
     * Generates and returns a [ColumnDef] given this [Projection] and the given input [ColumnDef].
     *
     * @param c The [ColumnDef] to create the output [ColumnDef] for.
     */
    fun columnDef(c: ColumnDef<*>) = when (this) {
        SELECT,
        SELECT_DISTINCT -> c
        COUNT,
        COUNT_DISTINCT -> {
            val name = "${this.name.lowercase()}_${c.name.simple}"
            ColumnDef(c.name.entity()?.column(name) ?: Name.ColumnName(name), Types.Long, true)
        }
        EXISTS -> {
            val name = "${this.name.lowercase()}_${c.name.simple}"
            ColumnDef(c.name.entity()?.column(name) ?: Name.ColumnName(name), Types.Boolean, true)
        }
        SUM,
        MAX,
        MIN,
        MEAN -> {
            val name = "${this.name.lowercase()}_${c.name.simple}"
            ColumnDef(c.name.entity()?.column(name) ?: Name.ColumnName(name), c.type, true)
        }
    }
}