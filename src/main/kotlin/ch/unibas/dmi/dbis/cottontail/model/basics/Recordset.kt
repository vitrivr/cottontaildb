package ch.unibas.dmi.dbis.cottontail.model.basics

import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate

import java.util.*

/**
 * A record set as returned and processed by Cottontail DB. [Recordset]s are tables. Their columns are defined by the [ColumnDef]'s
 * it contains ([Recordset.columns] and it contains an arbitrary number of [Record] entries as rows.
 *
 * [Recordset]s are the unit of data retrieval and processing in Cottontail DB. Whenever information is accessed through an [Entity],
 * a [Recordset] is being generated. Furthermore, the entire query execution pipeline processes and produces [Recordset]s.
 *
 * @see Entity
 * @see QueryExecutionTask
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class Recordset (val columns: Array<ColumnDef<*>>) {
    /** List of all the [Record]s contained in this [Recordset]. */
    private val list: LinkedList<Record> = LinkedList()

    /** The number of columns contained in this [Recordset]. */
    val columnCount: Int
        get() = this.columns.size

    /** The number of rows contained in this [Recordset]. */
    val rowCount: Int
        get() = this.list.size

    /**
     * Creates and appends a new [Record] (without a tupleId) given the provided values and appends them to this [Recordset].
     *
     * @param values The values to add to this [Recordset].
     */
    fun addRow(vararg values: Any?) {
        this.list.add(DatasetRecord().assign(values))
    }

    /**
     * Creates a new [Record] given the provided tupleId and values and appends them to this [Recordset].
     *
     * @param tupleId The tupleId of the new [Record].
     * @param values The values to add to this [Recordset].
     */
    fun addRow(tupleId: Long, vararg values: Any?) {
        this.list.add(DatasetRecord(tupleId).assign(*values))
    }

    /**
     * Creates a new [Record] given the provided tupleId and values and appends them to this [Recordset]
     * if they match the provided [BooleanPredicate].
     *
     * @param tupleId The tupleId of the new [Record].
     * @param predicate The [BooleanPredicate] to match the [Record] against
     * @param values The values to add to this [Recordset].
     * @return True if [Record] was added, false otherwise.
     */
    fun addRowIf(tupleId: Long, predicate: BooleanPredicate, vararg values: Any?): Boolean {
        val record = DatasetRecord(tupleId).assign(*values)
        return if (predicate.matches(record)) {
            this.list.add(record)
        } else {
            false
        }
    }

    /**
     * Apples the provided action to each [Record] in this [Recordset].
     *
     * @param action The action that should be applied.
     */
    fun forEach(action: (Record) -> Unit) = this.list.forEach(action)

    /**
     * Returns a list of all the [Record]s held by this
     */
    fun toList(): List<Record> = this.list

    /**
     * A [Record] implementation that depends on the existence of the enclosing [Recordset].
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class DatasetRecord(override val tupleId: Long? = null, init: Array<Any?>? = null) : Record {

        /** Array of [ColumnDef]s that describes the [Columns] of this [Record]. */
        override val columns: Array<out ColumnDef<*>>
            get() = this@Recordset.columns

        /** Array of column values (one entry per column). Initializes with null. */
        override val values: Array<Any?> = if (init != null) {
            init.forEachIndexed { index, any ->  columns[index].validateOrThrow(any) }
            init
        } else Array(columns.size) { columns[it].defaultValue() }

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
}