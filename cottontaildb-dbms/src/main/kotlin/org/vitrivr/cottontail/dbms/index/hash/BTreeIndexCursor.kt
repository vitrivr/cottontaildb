package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [Cursor] for the [BTreeIndex]. Different variants are implemented optimised for the respective [ComparisonOperator].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class BTreeIndexCursor(val predicate: BooleanPredicate.Atomic, val index: BTreeIndex.Tx, val context: TransactionContext) : Cursor<Record> {
    /** Internal cursor used for navigation. */
    protected val subTransaction = this.context.xodusTx.readonlySnapshot

    /** Internal cursor used for navigation. */
    protected val cursor: jetbrains.exodus.env.Cursor

    /** A begin of cursor flag. */
    protected val boc = AtomicBoolean(false)

    /* Perform initial sanity checks. */
    init {
        require(!predicate.not) { "NonUniqueHashIndex.filter() does not support negated statements." }
        this.cursor = this.index.dataStore.openCursor(this.subTransaction)
    }

    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.value)
    override fun value(): Record = StandaloneRecord(this.key(), this.index.columns, arrayOf(this.index.binding.entryToValue(this.cursor.key)))

    override fun close() {
        this.cursor.close()
        this.subTransaction.abort()
    }


    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.Binary.Equal] operators.
     */
    class Equals(predicate: BooleanPredicate.Atomic, index: BTreeIndex.Tx, context: TransactionContext): BTreeIndexCursor(predicate, index, context) {
        /* Perform initial sanity checks. */
        init {
            require(predicate.operator is ComparisonOperator.Binary.Equal) { "BTreeIndexCursor.Equals does only support the EQUAL operator." }

            /** Initialize cursor. */
            if (this.cursor.getSearchKey(this.index.binding.valueToEntry((predicate.operator as ComparisonOperator.Binary.Equal).right.value)) != null) {
                this.boc.compareAndExchange(false, true)
            }
        }

        override fun moveNext(): Boolean
            = (this.boc.compareAndExchange(true, false) || (this.cursor.nextDup))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.In] operators.
     */
    class In(predicate: BooleanPredicate.Atomic, index: BTreeIndex.Tx, context: TransactionContext): BTreeIndexCursor(predicate, index, context) {

        /** Internal list of query values for IN predicate. */
        private val queryValueQueue = LinkedList<Value>()

        init {
            require(predicate.operator is ComparisonOperator.In) { "BTreeIndexCursor.In does only support the IN operator." }

            this.queryValueQueue.addAll((predicate.operator as ComparisonOperator.In).right.mapNotNull { it.value })

            while (queryValueQueue.size > 0) {
                if (this.cursor.getSearchKey(this.index.binding.valueToEntry(this.queryValueQueue.poll())) != null) {
                    this.boc.compareAndExchange(false, true)
                    break
                }
            }
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
    class GreaterEqual(predicate: BooleanPredicate.Atomic, index: BTreeIndex.Tx, context: TransactionContext): BTreeIndexCursor(predicate, index, context) {
        init {
            require(predicate.operator is ComparisonOperator.Binary.GreaterEqual) { "BTreeIndexCursor.Greater does only support the >= operator." }

            /** Initialize cursor. */
            if (this.cursor.getSearchKeyRange(this.index.binding.valueToEntry((predicate.operator as ComparisonOperator.Binary.GreaterEqual).right.value)) != null) {
                this.boc.compareAndExchange(false, true)
            }
        }

        override fun moveNext(): Boolean
                = (this.boc.compareAndExchange(true, false) || (this.cursor.next))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.Binary.GreaterEqual] operators.
     */
    class Greater(predicate: BooleanPredicate.Atomic, index: BTreeIndex.Tx, context: TransactionContext): BTreeIndexCursor(predicate, index, context) {
        init {
            require(predicate.operator is ComparisonOperator.Binary.Greater) { "BTreeIndexCursor.GreaterEqual does only support the > operator." }

            /** Initialize cursor. */
            val value = this.index.binding.valueToEntry((predicate.operator as ComparisonOperator.Binary.Greater).right.value)
            if (this.cursor.getSearchKeyRange(value) != null) {
                this.boc.compareAndExchange(false, true)
                while (this.cursor.key == value) {
                    if (!this.cursor.next) {
                        this.boc.compareAndExchange(true, false)
                        break
                    }
                }
            }
        }

        override fun moveNext(): Boolean
            = (this.boc.compareAndExchange(true, false) || (this.cursor.next))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate [ComparisonOperator.Binary.LessEqual] operators.
     */
    class LessEqual(predicate: BooleanPredicate.Atomic, index: BTreeIndex.Tx, context: TransactionContext): BTreeIndexCursor(predicate, index, context) {
        init {
            require(predicate.operator is ComparisonOperator.Binary.LessEqual) { "BTreeIndexCursor.LessEqual does only support the <= operator." }

            /** Initialize cursor. */
            if (this.cursor.getSearchKeyRange(this.index.binding.valueToEntry((predicate.operator as ComparisonOperator.Binary.GreaterEqual).right.value)) != null || this.cursor.last) {
                this.boc.compareAndExchange(false, true)
            }
        }

        override fun moveNext(): Boolean
            = (this.boc.compareAndExchange(true, false) || (this.cursor.prev))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate [ComparisonOperator.Binary.LessEqual] operators.
     */
    class Less(predicate: BooleanPredicate.Atomic, index: BTreeIndex.Tx, context: TransactionContext): BTreeIndexCursor(predicate, index, context) {
        init {
            require(predicate.operator is ComparisonOperator.Binary.Less) { "BTreeIndexCursor.Less does only support the < operator." }

            /** Initialize cursor. */
            val value = this.index.binding.valueToEntry((predicate.operator as ComparisonOperator.Binary.Greater).right.value)
            if (this.cursor.getSearchKeyRange(this.index.binding.valueToEntry((predicate.operator as ComparisonOperator.Binary.Less).right.value)) != null || this.cursor.last) {
                this.boc.compareAndExchange(false, true)
                while (this.cursor.key == value) {
                    if (!this.cursor.prev) {
                        this.boc.compareAndExchange(true, false)
                        break
                    }
                }
            }
        }

        override fun moveNext(): Boolean
            = (this.boc.compareAndExchange(true, false) || (this.cursor.prev))
    }
}