package ch.unibas.dmi.dbis.cottontail.database.queries

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.Distance
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.values.Value

/**
 * A general purpose [Predicate] that describes a Cottontail DB query. It can either operate on [Recordset]s or data read from an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class Predicate {
    /** An estimation of the operations required to apply this [Predicate] to a [Record]. */
    abstract val operations: Int

    /** Set of [ColumnDef] that are affected by this [Predicate]. */
    abstract val columns: Set<ColumnDef<*>>
}

/**
 * A boolean [Predicate] that can be used to compare a [Record] to a given value.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal sealed class BooleanPredicate : Predicate() {
    /**
     * Returns true, if the provided [Record] matches the [Predicate] and false otherwise.
     *
     * @param record The [Record] that should be checked against the predicate.
     */
    abstract fun matches(record: Record): Boolean

    /**
     * Converts this [BooleanPredicate] into a collection of [AtomicBooleanPredicate] that make up this [BooleanPredicate].
     *
     * @return Collection of [AtomicBooleanPredicate]s
     */
    abstract fun allAtomic(): Collection<AtomicBooleanPredicate<*>>
}

/**
 * A atomic [BooleanPredicate] that compares the column of a [Record] to a provided value (or a set of provided values).
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class AtomicBooleanPredicate<T : Value<*>>(private val column: ColumnDef<T>, val operator: ComparisonOperator, val not: Boolean = false, var values: Collection<Value<*>>) : BooleanPredicate() {

    init {
        if (operator == ComparisonOperator.IN) {
            values = values.toSet()
        }
    }

    override val operations: Int = 1
    override val columns: Set<ColumnDef<T>> = setOf(column)
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

    /**
     * Returns a list containing this [AtomicBooleanPredicate]
     *
     * @return Collection of this [AtomicBooleanPredicate]
     */
    override fun allAtomic() = listOf(this)
}

/**
 * A compound [BooleanPredicate] that connects two other [BooleanPredicate]s through a logical AND or OR connection.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class CompoundBooleanPredicate(val connector: ConnectionOperator, val p1: BooleanPredicate, val p2: BooleanPredicate) : BooleanPredicate() {
    override val columns: Set<ColumnDef<*>> = p1.columns + p2.columns
    override val operations = p1.operations + p2.operations
    override fun matches(record: Record): Boolean = when (connector) {
        ConnectionOperator.AND -> p1.matches(record) && p2.matches(record)
        ConnectionOperator.OR -> p1.matches(record) || p2.matches(record)
    }

    /**
     * Returns a list of the [AtomicBooleanPredicate]s that make up this [CompoundBooleanPredicate]
     *
     * @return Collection of [AtomicBooleanPredicate]s that make up this [CompoundBooleanPredicate]
     */
    override fun allAtomic() = p1.allAtomic() + p2.allAtomic()
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
internal data class KnnPredicate<T : Any>(val column: ColumnDef<T>, val k: Int, val query: Array<Number>, val distance: Distance, val weights: Array<Number>? = null) : Predicate() {
    init {
        /* Some basic sanity checks. */
        if (k <= 0) throw QueryException.QuerySyntaxException("The value of k for a kNN query cannot be smaller than one (is $k)s!")
        if (column.size != query.size) throw QueryException.QueryBindException("The size of the provided column ${column.name} (s_c=${column.size}) does not match the size of the query vector (s_q=${query.size}).")
        if (weights != null && column.size != weights.size) throw QueryException.QueryBindException("The size of the provided column ${column.name} (s_c=${column.size}) does not match the size of the weight vector (s_w=${query.size}).")
    }

    /**
     * Columns affected by this [KnnPredicate].
     */
    override val columns: Set<ColumnDef<*>> = setOf(column)

    /**
     * Number of operations required for this [KnnPredicate]. Calculated by applying the base operations
     * for the [Distance] to each vector components.
     *
     * If weights are used, this will be added to the cost.
     */
    override val operations: Int = distance.operations * query.size + (this.weights?.size ?: 0)

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

    /**
     * Returns the weights vector as [FloatArray].
     *
     * @return [FloatArray]
     */
    fun weightsAsFloatArray() = if (this.weights != null) {
        FloatArray(this.weights.size) { this.weights[it].toFloat() }
    } else {
        null
    }

    /**
     * Returns the weights vector as [DoubleArray].
     *
     * @return [DoubleArray]
     */
    fun weightsAsDoubleArray() = if (this.weights != null) {
        DoubleArray(this.weights.size) { this.weights[it].toDouble() }
    } else {
        null
    }

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