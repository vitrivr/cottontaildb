package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [Cursor] for the [BTreeIndex]. Different variants are implemented optimised for the respective [ComparisonOperator].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
sealed class BTreeIndexCursor(val index: AbstractBTreeIndex.Tx) : Cursor<Tuple> {
    /** Internal cursor used for navigation. */
    protected val xodusTx: Transaction = this.index.xodusTx.readonlySnapshot

    /** The Xodus [Store] used to store entries in the [BTreeIndex]. */
    protected val store: Store = this.xodusTx.environment.openStore(this.index.dbo.name.storeName(), StoreConfig.USE_EXISTING, this.xodusTx)

    /** The internal [ValueSerializer] reference used for de-/serialization. */
    @Suppress("UNCHECKED_CAST")
    protected val binding: ValueSerializer<Value> = SerializerFactory.value(this.index.columns[0].type) as ValueSerializer<Value>

    /** Internal cursor used for navigation. */
    protected val cursor: jetbrains.exodus.env.Cursor = this.store.openCursor(this.xodusTx)

    /** A beginning of cursor (BoC) flag. */
    protected val boc = AtomicBoolean(false)

    /** Flag indicating, that this [BTreeIndexCursor] is empty. */
    protected val empty: Boolean

    /* Perform initial sanity checks. */
    init {
        this@BTreeIndexCursor.boc.compareAndExchange(false, this@BTreeIndexCursor.initialize())
        this@BTreeIndexCursor.empty = !this@BTreeIndexCursor.boc.get()
    }

    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.value)
    override fun value(): Tuple = StandaloneTuple(this.key(), this.index.columns, arrayOf(this.binding.fromEntry(this.cursor.key)))

    override fun close() {
        this.cursor.close()
        this.xodusTx.abort()
    }

    protected abstract fun initialize(): Boolean

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.Equal] operators.
     */
    class Equals(index: AbstractBTreeIndex.Tx, private val value: Value): BTreeIndexCursor(index) {
        override fun initialize(): Boolean = this.cursor.getSearchKey(this.binding.toEntry(this.value)) != null
        override fun moveNext(): Boolean = !this.empty && (this.boc.compareAndExchange(true, false) || (this.cursor.nextDup))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.GreaterEqual] operators.
     */
    class GreaterEqual(index: AbstractBTreeIndex.Tx, private val value: Value): BTreeIndexCursor(index) {
        override fun initialize(): Boolean = this.cursor.getSearchKeyRange(this.binding.toEntry(this.value)) != null
        override fun moveNext(): Boolean = !this.empty && (this.boc.compareAndExchange(true, false) || (this.cursor.next))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.GreaterEqual] operators.
     */
    class Greater(index: AbstractBTreeIndex.Tx, private val value: Value): BTreeIndexCursor(index) {
        override fun initialize(): Boolean {
            val value = this.binding.toEntry(this.value)
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
    class LessEqual(index: AbstractBTreeIndex.Tx, private val value: Value): BTreeIndexCursor(index) {
        override fun initialize(): Boolean = this.cursor.getSearchKeyRange(this.binding.toEntry(this.value)) != null
        override fun moveNext(): Boolean = !this.empty && (this.boc.compareAndExchange(true, false) || (this.cursor.prev))
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate [ComparisonOperator.LessEqual] operators.
     */
    class Less(index: AbstractBTreeIndex.Tx, private val value: Value): BTreeIndexCursor(index) {
        override fun initialize(): Boolean {
            val value = this.binding.toEntry(this.value)
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

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.In] operators.
     */
    class In(index: AbstractBTreeIndex.Tx, private val values: LinkedList<Value>): BTreeIndexCursor(index) {
        override fun initialize(): Boolean {
            while (this.values.isNotEmpty()) {
                if (this.cursor.getSearchKey(this.binding.toEntry(this.values.poll())) != null) {
                    return true
                }
            }
            return false
        }

        override fun moveNext(): Boolean {
            if ((this.boc.compareAndExchange(true, false) || (this.cursor.nextDup))) return true
            while (this.values.size > 0) {
                if (this.cursor.getSearchKey(this.binding.toEntry(this.values.poll())) != null) {
                    return true
                }
            }
            return false
        }
    }
}