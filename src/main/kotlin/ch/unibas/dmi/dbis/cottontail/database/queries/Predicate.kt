package ch.unibas.dmi.dbis.cottontail.database.queries

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.Distance
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
internal data class AtomicBooleanPredicate<T: Any>(val column: ColumnDef<T>, val operator: ComparisonOperator, val not: Boolean = false, val values: Array<T>): BooleanPredicate() {
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

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as AtomicBooleanPredicate<*>

        if (column != other.column) return false
        if (operator != other.operator) return false
        if (not != other.not) return false
        if (!values.contentEquals(other.values)) return false
        if (columns != other.columns) return false

        return true
    }

    override fun hashCode(): Int {
        var result = column.hashCode()
        result = 31 * result + operator.hashCode()
        result = 31 * result + not.hashCode()
        result = 31 * result + values.contentHashCode()
        result = 31 * result + columns.hashCode()
        return result
    }
}

/**
 * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical AND or OR connection.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class CompoundBooleanPredicate(val connector: ConnectionOperator, val p1: BooleanPredicate, val p2: BooleanPredicate): BooleanPredicate() {
    override val columns: Set<ColumnDef<*>> = mutableSetOf()
    init {
        columns.plus(p1.columns)
        columns.plus(p2.columns)
    }
    override fun matches(record: Record): Boolean = when(connector) {
        ConnectionOperator.AND -> p1.matches(record) && p2.matches(record)
        ConnectionOperator.OR -> p1.matches(record) || p2.matches(record)
    }
}

/**
 * A k nearest neighbour (kNN) lookup [Predicate]. It can be used to compare the distance between database [Record] and given a query
 * vector and select the closes k entries.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class  KnnPredicate<T: Any>(val column: ColumnDef<T>, val k: Int, val query: Array<Number>, val distance: Distance, val weights: Array<Number>? = null): Predicate() {
    init {
        /* Some basic sanity checks. */
        if (k <= 0) throw QueryException.QuerySyntaxException("The value of k for a kNN query cannot be smaller than one (is $k)s!")
        if (column.size != query.size) throw QueryException.QueryBindException("The size of the provided column ${column.name} (s_c=${column.size}) does not match the size of the query vector (s_q=${query.size}).")
        if (weights != null && column.size != weights.size) throw QueryException.QueryBindException("The size of the provided column ${column.name} (s_c=${column.size}) does not match the size of the weight vector (s_w=${query.size}).")
    }

    /**
     * Returns the query vector as [FloatArray].
     *
     * @return [FloatArray]
     */
    fun queryAsFloatArray() = FloatArray(this.query.size) { this.query[it].toFloat() }

    /**
     * Returns the query vector as [DoubleArray].
     *
     * @return [DoubleArray]
     */
    fun queryAsDoubleArray() = DoubleArray(this.query.size) { this.query[it].toDouble() }

    /**
     * Returns the query vector as [IntArray].
     *
     * @return [IntArray]
     */
    fun queryAsIntArray() = IntArray(this.query.size) { this.query[it].toInt() }

    /**
     * Returns the query vector as [LongArray].
     *
     * @return [LongArray]
     */
    fun queryAsLongArray() = LongArray(this.query.size) { this.query[it].toLong() }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as KnnPredicate<*>

        if (column != other.column) return false
        if (k != other.k) return false
        if (!query.contentEquals(other.query)) return false
        if (distance != other.distance) return false
        if (weights != null) {
            if (other.weights == null) return false
            if (!weights.contentEquals(other.weights)) return false
        } else if (other.weights != null) return false

        return true
    }

    override fun hashCode(): Int {
        var result = column.hashCode()
        result = 31 * result + k
        result = 31 * result + query.contentHashCode()
        result = 31 * result + distance.hashCode()
        result = 31 * result + (weights?.contentHashCode() ?: 0)
        return result
    }
}