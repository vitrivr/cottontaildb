package org.vitrivr.cottontail.client.iterators

import io.grpc.Context
import org.vitrivr.cottontail.client.language.extensions.parse
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.toType
import org.vitrivr.cottontail.core.toValue
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*
import java.util.concurrent.CancellationException

/**
 * A [TupleIterator] used for retrieving [Tuple]s in a synchronous fashion.
 *
 * Usually used with unary, server-side calls that only return a limited amount of data.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class TupleIteratorImpl internal constructor(private val results: Iterator<CottontailGrpc.QueryResponseMessage>, private val context: Context.CancellableContext) : TupleIterator {

    /** Constructor for single [CottontailGrpc.QueryResponseMessage]. */
    constructor(result: CottontailGrpc.QueryResponseMessage, context: Context.CancellableContext): this(sequenceOf(result).iterator(), context)

    /** Internal buffer with pre-loaded [CottontailGrpc.QueryResponseMessage.Tuple]. */
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
    override val columns: List<ColumnDef<*>>

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
        val list = ArrayList<ColumnDef<*>>(next.columnsList.size)
        next.columnsList.forEach { c ->
            list.add(ColumnDef(c.name.parse(), c.type.toType(c.length), c.nullable, c.primary, c.autoIncrement))
        }
        this.columns = list

        /* Parse first batch of tuples. */
        next.tuplesList.forEach { t->
            this.buffer.add(Tuple(this.columns, t.dataList.map { l -> l.toValue() }))
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
                this.buffer.add(Tuple(this.columns, t.dataList.map { l -> l.toValue() }))
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
}