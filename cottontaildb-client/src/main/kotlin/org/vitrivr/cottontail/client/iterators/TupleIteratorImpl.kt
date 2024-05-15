package org.vitrivr.cottontail.client.iterators

import io.grpc.Context
import org.vitrivr.cottontail.client.language.extensions.parse
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.toType
import org.vitrivr.cottontail.core.toValue
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*
import java.util.concurrent.CancellationException

/**
 * A [TupleIterator] used for retrieving [Tuple]s in a synchronous fashion.
 *
 * Usually used with unary, server-side calls that only return a limited amount of data.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class TupleIteratorImpl internal constructor(private val results: Iterator<CottontailGrpc.QueryResponseMessage>, private val context: Context.CancellableContext) : TupleIterator {

    /** Constructor for single [CottontailGrpc.QueryResponseMessage]. */
    constructor(result: CottontailGrpc.QueryResponseMessage, context: Context.CancellableContext): this(sequenceOf(result).iterator(), context)

    /** Internal buffer with pre-loaded [Tuple]s. */
    private val buffer = LinkedList<Tuple>()

    /** The ID of the Cottontail DB transaction this [TupleIterator] is associated with. */
    override val transactionId: Long

    /** The ID of the Cottontail DB query this [TupleIterator] is associated with. */
    override val queryId: String

    /** The time it took in milliseconds to plan the query. */
    override val planDuration: Long

    /** The time it took in milliseconds to execute the query. */
    override val queryDuration: Long

    /** The column names returned by this [TupleIterator]. */
    override val columns: Array<ColumnDef<*>>

    /** The index of the last row returned. */
    private var rowIndex = 0L

    /** Returns the number of columns contained in the [Tuple]s returned by this [TupleIterator]. */
    override val numberOfColumns: Int
        get() = this.columns.size

    init {
        /* Start loading first results. */
        val next = this.results.next()

        /* Assign metadata, columns and data. */
        this.transactionId = next.metadata.transactionId
        this.queryId = next.metadata.queryId
        this.planDuration = next.metadata.planDuration
        this.queryDuration = next.metadata.queryDuration

        /* Parse list of columns. */
        this.columns = List(next.columnsList.size) {
            val c = next.columnsList[it]
            ColumnDef(c.name.parse(), c.type.toType(c.length), c.nullable, c.primary, c.autoIncrement)
        }.toTypedArray()

        /* Parse first batch of tuples. */
        next.tuplesList.forEach { t->
            this.buffer.add(IteratorTuple(Array<Value?>(this.numberOfColumns) { t.dataList[it].toValue() }))
        }

        /** Call finalizer if no more data is available. */
        if (!this.results.hasNext()) {
            this.context.close()
        }
    }

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun hasNext(): Boolean {
        if (this.buffer.isNotEmpty()) return true
        if (!this.results.hasNext()) {
            this.context.close()
            return false
        }
        return true
    }

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun next(): Tuple {
        if (this.buffer.isEmpty()) {
            if (!this.results.hasNext()) {
                /* Should never be reached. */
                this.context.close()
                throw IllegalArgumentException("TupleIterator has been drained and no more elements can be loaded. Call hasNext() to ensure that elements are available before calling next().")
            }
            this.results.next().tuplesList.map { t ->
                this.buffer.add(IteratorTuple(Array(this.numberOfColumns) { t.dataList[it].toValue() }))
            }
        }
        return this.buffer.poll()!!
    }

    /**
     * Closes this [TupleIteratorImpl].
     */
    override fun close() {
        this.context.cancel(CancellationException("TupleIterator has been prematurely closed by user."))
    }

    /**
     * Internal [Tuple] implementation.
     */
    inner class IteratorTuple(val values: Array<Value?>): Tuple {
        override val tupleId: TupleId = this@TupleIteratorImpl.rowIndex++
        override val columns: Array<ColumnDef<*>>
            get() = this@TupleIteratorImpl.columns
        override fun copy(): Tuple = StandaloneTuple(this.tupleId, this.columns, this.values.copyOf())
        override fun get(index: Int): Value? = this.values[index]
        override fun values(): List<Value?> = this.values.toList()
    }
}