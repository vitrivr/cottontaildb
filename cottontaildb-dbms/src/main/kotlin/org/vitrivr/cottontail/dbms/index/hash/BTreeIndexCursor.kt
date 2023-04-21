package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.binding.MissingRecord
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.Value
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [Cursor] for the [BTreeIndex]. Different variants are implemented optimised for the respective [ComparisonOperator].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class BTreeIndexCursor<T: ComparisonOperator>(val operator: T, val index: BTreeIndex.Tx) : Cursor<Record> {
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
            with(MissingRecord) {
                this@BTreeIndexCursor.boc.compareAndExchange(false, this@BTreeIndexCursor.initialize())
                this@BTreeIndexCursor.empty = !this@BTreeIndexCursor.boc.get()
            }
        }
    }

    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.value)
    override fun value(): Record = StandaloneRecord(this.key(), this.index.columns, arrayOf(this.index.binding.entryToValue(this.cursor.key)))

    override fun close() {
        this.cursor.close()
        this.subTransaction.abort()
    }

    context(BindingContext,Record)
    protected abstract fun initialize(): Boolean

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.Binary.Equal] operators.
     */
    class Equals(operator: ComparisonOperator.Binary.Equal, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.Binary.Equal>(operator, index) {
        context(BindingContext,Record)
        override fun initialize(): Boolean = this@Equals.cursor.getSearchKey(this@Equals.index.binding.valueToEntry(this.operator.right.getValue())) != null
        override fun moveNext(): Boolean = !this.empty && (this.boc.compareAndExchange(true, false) || (this.cursor.nextDup))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.In] operators.
     */
    class In(operator: ComparisonOperator.In, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.In>(operator, index) {

        /** Internal list of query values for IN predicate. */
        private val queryValueQueue = LinkedList<Value>()

        context(BindingContext,Record)
        override fun initialize(): Boolean {
            this.queryValueQueue.addAll(this.operator.right.mapNotNull { it.getValue() })
            while (this.queryValueQueue.size > 0) {
                if (this@In.cursor.getSearchKey(this@In.index.binding.valueToEntry(this@In.queryValueQueue.poll())) != null) {
                    return true
                }
            }
            return false
        }

        override fun moveNext(): Boolean {
            if ((this.boc.compareAndExchange(true, false) || (this.cursor.nextDup))) return true
            while (this.queryValueQueue.size > 0) {
                if (this.cursor.getSearchKey(this.index.binding.valueToEntry(this.queryValueQueue.poll())) != null) {
                    return true
                }
            }
            return false
        }
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.Binary.GreaterEqual] operators.
     */
    class GreaterEqual(operator: ComparisonOperator.Binary.GreaterEqual, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.Binary.GreaterEqual>(operator, index) {
        context(BindingContext,Record)
        override fun initialize(): Boolean = this.cursor.getSearchKeyRange(this.index.binding.valueToEntry(this.operator.right.getValue())) != null
        override fun moveNext(): Boolean = !this.empty && (this.boc.compareAndExchange(true, false) || (this.cursor.next))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.Binary.GreaterEqual] operators.
     */
    class Greater(operator: ComparisonOperator.Binary.Greater, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.Binary.Greater>(operator, index) {
        context(BindingContext,Record)
        override fun initialize(): Boolean {
            val value = this.index.binding.valueToEntry(this.operator.right.getValue())
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
     * A [BTreeIndexCursor] variant to evaluate [ComparisonOperator.Binary.LessEqual] operators.
     */
    class LessEqual(operator: ComparisonOperator.Binary.LessEqual, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.Binary.LessEqual>(operator, index) {
        context(BindingContext,Record)
        override fun initialize(): Boolean = this.cursor.getSearchKeyRange(this.index.binding.valueToEntry(this.operator.right.getValue())) != null

        override fun moveNext(): Boolean = !this.empty && (this.boc.compareAndExchange(true, false) || (this.cursor.prev))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate [ComparisonOperator.Binary.LessEqual] operators.
     */
    class Less(operator: ComparisonOperator.Binary.Less, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.Binary.Less>(operator, index) {
        context(BindingContext,Record)
        override fun initialize(): Boolean {
            val value = this.index.binding.valueToEntry(this.operator.right.getValue())
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