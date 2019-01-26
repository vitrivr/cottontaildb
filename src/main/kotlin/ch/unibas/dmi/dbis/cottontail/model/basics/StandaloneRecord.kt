package ch.unibas.dmi.dbis.cottontail.model.basics

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef

/**
 * A [Record] implementation as returned and processed by Cottontail DB. These types of records can exist
 * without an enclosing [Recordset] and are necessary for some applications.
 *
 * <strong>Important:</strong> The use of [StandaloneRecord] is discouraged when data volume becomes large,
 * as each [StandaloneRecord] has its own reference to the [ColumnDef]s it contains.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class StandaloneRecord(override val tupleId: Long? = null, override vararg val columns: ColumnDef<*>) : Record {
    /** Array of column values (one entry per column). Initializes with null. */
    override val values: Array<Any?> = Array(columns.size, { })

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Record

        if (tupleId != other.tupleId) return false
        if (!columns.contentEquals(other.columns)) return false
        if (!values.contentEquals(other.values)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = tupleId.hashCode()
        result = 31 * result + columns.hashCode()
        result = 31 * result + values.contentHashCode()
        return result
    }
}