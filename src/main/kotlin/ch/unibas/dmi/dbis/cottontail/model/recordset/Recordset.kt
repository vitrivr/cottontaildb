package ch.unibas.dmi.dbis.cottontail.model.recordset

import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Filterable
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.basics.Scanable
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.values.Value

import java.util.*
import java.util.concurrent.atomic.AtomicLong

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
class Recordset(val columns: Array<ColumnDef<*>>) : Scanable, Filterable {

    /** List of all the [Record]s contained in this [Recordset]. */
    private val list: LinkedList<Record> = LinkedList()

    /** Internal counter for maximum tupleId. */
    private val maxTupleId = AtomicLong(0)

    /** The number of columns contained in this [Recordset]. */
    val columnCount: Int
        get() = this.columns.size

    /** The number of rows contained in this [Recordset]. */
    val rowCount: Int
        get() = this.list.size

    /**
     * Creates and appends a new [Record] (without a tupleId) given the provided values and appends it to this [Recordset].
     *
     * @param values The values to add to this [Recordset].
     */
    fun addRow(values: Array<Value<*>?>) {
        this.list.add(DatasetRecord(this.maxTupleId.incrementAndGet()).assign(values))
    }

    /**
     * Creates a new [Record] given the provided tupleId and values and appends it to this [Recordset].
     *
     * @param tupleId The tupleId of the new [Record].
     * @param values The values to add to this [Recordset].
     */
    fun addRow(tupleId: Long, values: Array<Value<*>?>) {
        this.list.add(DatasetRecord(tupleId).assign(values))
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
    fun addRowIf(tupleId: Long, predicate: BooleanPredicate, values: Array<Value<*>?>): Boolean {
        val record = DatasetRecord(tupleId).assign(values)
        return if (predicate.matches(record)) {
            this.list.add(record)
        } else {
            false
        }
    }

    /**
     * Retrieves the value for the specified [ColumnDef] from this [Record].
     *
     * @param column The [ColumnDef] for which to retrieve the value.
     * @return The value for the [ColumnDef]
     */
    operator fun get(index: Int): Record = this.list[index]

    /**
     * Applies the provided action to each [Record] in this [Recordset].
     *
     * @param action The action that should be applied.
     */
    override fun forEach(action: (Record) -> Unit) = this.list.forEach(action)

    /**
     * Applies the provided mapping function to each [Record] in this [Recordset].
     *
     * @param action The mapping function that should be applied.
     */
    override fun <R> map(action: (Record) -> R): Collection<R> = this.list.map(action)

    /**
     * Applies the provided action to each [Record] that matches the given [Predicate].
     *
     * @param action The action that should be applied.
     */
    override fun forEach(predicate: Predicate, action: (Record) -> Unit) {
        if (predicate is BooleanPredicate) {
            this.list.asSequence().filter { predicate.matches(it) }.forEach { action(it) }
        } else {
            throw QueryException.UnsupportedPredicateException("Only boolean predicates are supported for invocation of forEach() on a Recordset.")
        }
    }

    /**
     * Applies the provided mapping function to each [Record] that matches the given [Predicate].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @param action The mapping function that should be applied.
     */
    override fun <R> map(predicate: Predicate, action: (Record) -> R): Collection<R> {
        if (predicate is BooleanPredicate) {
            val list = mutableListOf<R>()
            this.list.asSequence().filter { predicate.matches(it) }.map(action).forEach { list.add(it) }
            return list
        } else {
            throw QueryException.UnsupportedPredicateException("Only boolean predicates are supported for invocation of map() on a Recordset.")
        }
    }

    /**
     * Filters this [Filterable] thereby creating and returning a new [Filterable].
     *
     * @param predicate [Predicate] to filter [Record]s.
     * @return New [Filterable]
     */
    override fun filter(predicate: Predicate): Recordset {
        if (predicate is BooleanPredicate) {
            val recordset = Recordset(this.columns)
            this.list.asSequence().filter { predicate.matches(it) }.forEach { recordset.addRow(it.values) }
            return recordset
        } else {
            throw QueryException.UnsupportedPredicateException("Only boolean predicates are supported for invocation of filter() on a Recordset.")
        }
    }

    /**
     * Returns a list view of this [Recordset]
     *
     * @return The [List] underpinning this [Recordset].
     */
    fun toList(): List<Record> = this.list

    /**
     * Returns an [Iterator] of this [Recordset]
     *
     * @return [Iterator] of this [Recordset].
     */
    fun iterator(): Iterator<Record> = this.list.iterator()

    /**
     * A [Record] implementation that depends on the existence of the enclosing [Recordset].
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class DatasetRecord(override val tupleId: Long, init: Array<Value<*>?>? = null) : Record {

        /** Array of [ColumnDef]s that describes the [Columns] of this [Record]. */
        override val columns: Array<out ColumnDef<*>>
            get() = this@Recordset.columns

        /** Array of column values (one entry per column). Initializes with null. */
        override val values: Array<Value<*>?> = if (init != null) {
            init.forEachIndexed { index, any -> columns[index].validateOrThrow(any) }
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