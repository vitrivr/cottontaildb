package ch.unibas.dmi.dbis.cottontail.database.queries

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException

/**
 * A general purpose [Predicate] that describes a Cottontail DB query. It can either operate on [Recordset]s or data read from an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class Predicate

/**
 * A boolean [Predicate] that can be used to compare a [Record] to a given value.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class BooleanPredicate: Predicate() {
    /**
     * Set of [ColumnDef] that are inspected by this [BooleanPredicate].
     */
    abstract val columns: Set<ColumnDef<*>>

    /**
     * Returns true, if the provided [Record] matches the [Predicate] and false otherwise.
     *
     * @param record The [Record] that should be checked against the predicate.
     */
    abstract fun matches(record: Record): Boolean
}

/**
 * A atomic [BooleanPredicate] that compares the column of a [Record] to a provided value (or a set of provided values).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
data class AtomicBooleanPredicate<T: Any>(val column: ColumnDef<T>, val operator: Operator, val not: Boolean = false, val values: Array<T>): BooleanPredicate() {
    override val columns: Set<ColumnDef<*>> = setOf(column)
    override fun matches(record: Record): Boolean {
        if (record.has(column)) {
            return if (not) {
                !operator.match(record[column], values)
            } else {
                operator.match(record[column], values)
            }
        } else {
            throw QueryException.ColumnDoesNotExistException(column)
        }
    }
}

/**
 * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical AND or OR connection.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
data class CompoundBooleanPredicate(val connector: Connector, val p1: BooleanPredicate, val p2: BooleanPredicate): BooleanPredicate() {
    override val columns: Set<ColumnDef<*>> = mutableSetOf()
    init {
        columns.plus(p1.columns)
        columns.plus(p2.columns)
    }
    override fun matches(record: Record): Boolean = when(connector) {
        Connector.AND -> p1.matches(record) && p2.matches(record)
        Connector.OR -> p1.matches(record) || p2.matches(record)
    }
}

/**
 *
 */
data class KnnPredicate<T: Any>(val column: ColumnDef<T>, val k: Int, val value: Array<Number>): Predicate()