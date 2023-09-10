package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [Cursor] for the [BTreeIndex]. Different variants are implemented optimised for the respective [ComparisonOperator].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class BTreeIndexCursor<T: ComparisonOperator>(val operator: T, val index: BTreeIndex.Tx) : Cursor<Tuple> {
    /** Internal cursor used for navigation. */
    protected val subTransaction = this.index.context.txn.xodusTx.readonlySnapshot

    /** Internal cursor used for navigation. */
    protected val cursor: jetbrains.exodus.env.Cursor

    /** A beginning of cursor (BoC) flag. */
    protected val boc = AtomicBoolean(false)

    /** Flag indicating, that this [BTreeIndexCursor] is empty. */
    protected val empty: Boolean

    /* Perform initial sanity checks. */
    init {
        this.cursor = this.index.dataStore.openCursor(this.subTransaction)
        with(this@BTreeIndexCursor.index.context.bindings) {
            with(MissingTuple) {
                this@BTreeIndexCursor.boc.compareAndExchange(false, this@BTreeIndexCursor.initialize())
                this@BTreeIndexCursor.empty = !this@BTreeIndexCursor.boc.get()
            }
        }
    }

    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.value)
    override fun value(): Tuple = StandaloneTuple(this.key(), this.index.columns, arrayOf(this.index.binding.fromEntry(this.cursor.key)))

    override fun close() {
        this.cursor.close()
        this.subTransaction.abort()
    }

    context(BindingContext, Tuple)
    protected abstract fun initialize(): Boolean

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.Equal] operators.
     */
    class Equals(operator: ComparisonOperator.Equal, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.Equal>(operator, index) {
        context(BindingContext, Tuple)
        override fun initialize(): Boolean = this@Equals.cursor.getSearchKey(this@Equals.index.binding.toEntry(this.operator.right.getValue()!!)) != null
        override fun moveNext(): Boolean = !this.empty && (this.boc.compareAndExchange(true, false) || (this.cursor.nextDup))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.In] operators.
     */
    class In(operator: ComparisonOperator.In, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.In>(operator, index) {

        /** Internal list of query values for IN predicate. */
        private val queryValueQueue = LinkedList<Value>()

        context(BindingContext, Tuple)
        override fun initialize(): Boolean {
            this.queryValueQueue.addAll(this.operator.right.getValues().filterNotNull())
            while (this.queryValueQueue.size > 0) {
                if (this@In.cursor.getSearchKey(this@In.index.binding.toEntry(this@In.queryValueQueue.poll())) != null) {
                    return true
                }
            }
            return false
        }

        override fun moveNext(): Boolean {
            if ((this.boc.compareAndExchange(true, false) || (this.cursor.nextDup))) return true
            while (this.queryValueQueue.size > 0) {
                if (this.cursor.getSearchKey(this.index.binding.toEntry(this.queryValueQueue.poll())) != null) {
                    return true
                }
            }
            return false
        }
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.GreaterEqual] operators.
     */
    class GreaterEqual(operator: ComparisonOperator.GreaterEqual, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.GreaterEqual>(operator, index) {
        context(BindingContext, Tuple)
        override fun initialize(): Boolean = this.cursor.getSearchKeyRange(this.index.binding.toEntry(this.operator.right.getValue()!!)) != null
        override fun moveNext(): Boolean = !this.empty && (this.boc.compareAndExchange(true, false) || (this.cursor.next))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.GreaterEqual] operators.
     */
    class Greater(operator: ComparisonOperator.Greater, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.Greater>(operator, index) {
        context(BindingContext, Tuple)
        override fun initialize(): Boolean {
            val value = this.index.binding.toEntry(this.operator.right.getValue()!!)
            if (this.cursor.getSearchKeyRange(value) != null) {
                while (this.cursor.key == value) {
                    if (!this.cursor.next) {
                        return false
                    }
                }
                return true
            }
            return false
        }

        override fun moveNext(): Boolean = !this.empty && (this.boc.compareAndExchange(true, false) || (this.cursor.next))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate [ComparisonOperator.LessEqual] operators.
     */
    class LessEqual(operator: ComparisonOperator.LessEqual, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.LessEqual>(operator, index) {
        context(BindingContext, Tuple)
        override fun initialize(): Boolean = this.cursor.getSearchKeyRange(this.index.binding.toEntry(this.operator.right.getValue()!!)) != null

        override fun moveNext(): Boolean = !this.empty && (this.boc.compareAndExchange(true, false) || (this.cursor.prev))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate [ComparisonOperator.LessEqual] operators.
     */
    class Less(operator: ComparisonOperator.Less, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.Less>(operator, index) {
        context(BindingContext, Tuple)
        override fun initialize(): Boolean {
            val value = this.index.binding.toEntry(this.operator.right.getValue()!!)
            if (this.cursor.getSearchKeyRange(value) != null) {
                while (this.cursor.key == value) {
                    if (!this.cursor.prev) {
                        return false
                    }
                }
                return true
            }
            return false
        }

        override fun moveNext(): Boolean = !this.empty && (this.boc.compareAndExchange(true, false) || (this.cursor.prev))
    }
}