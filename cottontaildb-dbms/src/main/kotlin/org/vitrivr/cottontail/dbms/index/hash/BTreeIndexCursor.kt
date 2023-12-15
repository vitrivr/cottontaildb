package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import java.util.*

/**
 * A [Cursor] for the [BTreeIndex]. Different variants are implemented optimised for the respective [ComparisonOperator].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class BTreeIndexCursor<T: ComparisonOperator>(val index: BTreeIndex.Tx) : Cursor<Tuple> {
    /** Internal cursor used for navigation. */
    private val subTransaction = this.index.context.txn.xodusTx.readonlySnapshot

    /** Internal cursor used for navigation. */
    protected val cursor: jetbrains.exodus.env.Cursor = this.index.dataStore.openCursor(this.subTransaction)

    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.value)
    override fun value(): Tuple = StandaloneTuple(this.key(), this.index.columns, arrayOf(this.index.binding.fromEntry(this.cursor.key)))

    override fun close() {
        this.cursor.close()
        this.subTransaction.abort()
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.Equal] operators.
     */
    class Equals(private val value: Value, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.Equal>(index) {
        override fun moveNext(): Boolean = try {
            this.cursor.nextDup
        } catch (e: IllegalStateException) {
            this@Equals.cursor.getSearchKey(this@Equals.index.binding.toEntry(this.value)) != null
        }
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.In] operators.
     */
    class In(values: List<Value?>, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.In>(index) {

        /** List of [ByteIterable]s to search for. */
        private val values = LinkedList<ByteIterable>()

        init {
            for (v in values) {
                if (v != null) this.values.add(this.index.binding.toEntry(v))
            }
            this.values.sort()
        }

        override fun moveNext(): Boolean {
            try {
                if (this.cursor.nextDup) return true
            } catch (_: IllegalStateException) {

            }

            /* Seek next value */
            while (this.values.size > 0) {
                if (this.cursor.getSearchKey(this.values.poll()) != null) {
                    return true
                }
            }
            return false
        }
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.GreaterEqual] operators.
     */
    class GreaterEqual(private val value: Value, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.GreaterEqual>(index) {
        init {
            this.cursor.getSearchKeyRange(this.index.binding.toEntry(this.value))
        }
        override fun moveNext(): Boolean = this.cursor.next
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.GreaterEqual] operators.
     */
    class Greater(private val value: Value, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.Greater>(index) {

        init {
            val entry = this.index.binding.toEntry(this.value)
            if (this.cursor.getSearchKeyRange(entry) != null) {
                while (this.cursor.key == entry && this.cursor.next) {
                    /* NOOP */
                }
            }
        }
        override fun moveNext(): Boolean = this.cursor.next
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate [ComparisonOperator.LessEqual] operators.
     */
    class LessEqual(private val value: Value, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.LessEqual>(index) {
        init {
            this.cursor.getSearchKeyRange(this.index.binding.toEntry(this.value))
        }
        override fun moveNext(): Boolean = this.cursor.prev
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate [ComparisonOperator.LessEqual] operators.
     */
    class Less(private val value: Value, index: BTreeIndex.Tx): BTreeIndexCursor<ComparisonOperator.Less>(index) {
        init {
            val entry = this.index.binding.toEntry(this.value)
            if (this.cursor.getSearchKeyRange(entry) != null) {
                while (this.cursor.key == entry && this.cursor.prev) {
                    /* NOOP */
                }
            }
        }

        override fun moveNext(): Boolean = this.cursor.prev
    }
}