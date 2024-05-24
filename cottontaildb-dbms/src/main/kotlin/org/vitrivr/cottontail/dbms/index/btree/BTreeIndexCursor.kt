package org.vitrivr.cottontail.dbms.index.btree

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.serialization.buffer.ValueSerializer
import org.vitrivr.cottontail.storage.serializers.ValueBinding
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [Cursor] for the [BTreeIndex]. Different variants are implemented optimised for the respective [ComparisonOperator].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class BTreeIndexCursor<T: ComparisonOperator>(protected val index: BTreeIndex.Tx, val columns: Array<ColumnDef<*>>) : Cursor<Tuple> {
    /** Internal cursor used for navigation. */
    private val xodusTx = this.index.xodusTx.readonlySnapshot

    /** The Xodus [Store] used to store entries in the [BTreeIndex]. */
    private val store = this.xodusTx.environment.openStore(this.index.dbo.name.storeName(), StoreConfig.USE_EXISTING, this.xodusTx, false)
        ?: throw DatabaseException.DataCorruptionException("Data store for index ${this.index.dbo.name} is missing.")

    /** The internal [ValueBinding] reference used for de-/serialization. */
    @Suppress("UNCHECKED_CAST")
    protected val binding: ValueBinding<Value> = ValueBinding(ValueSerializer.serializer(this.columns[0].type)) as ValueBinding<Value>

    /** Internal cursor used for navigation. */
    protected val cursor: jetbrains.exodus.env.Cursor = this.store.openCursor(this.xodusTx)

    /** A beginning of cursor flag. */
    protected val boc = AtomicBoolean(true)

    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.value)

    override fun value(): Tuple = this.index.parent.read(this.key())

    override fun close() {
        this.cursor.close()
        this.xodusTx.abort()
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.Equal] operators.
     */
    class Equals(private val value: Value, index: BTreeIndex.Tx, columns: Array<ColumnDef<*>>): BTreeIndexCursor<ComparisonOperator.Equal>(index, columns) {
        override fun moveNext(): Boolean {
            return if (this.boc.getAndSet(false)) {
                return this.cursor.getSearchKey(this.binding.toEntry(this.value)) != null
            } else {
                this.cursor.nextDup
            }
        }
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.In] operators.
     */
    class In(values: List<Value?>, index: BTreeIndex.Tx, columns: Array<ColumnDef<*>>): BTreeIndexCursor<ComparisonOperator.In>(index, columns) {

        /** List of [ByteIterable]s to search for. */
        private val values = LinkedList<ByteIterable>()

        init {
            for (v in values) {
                if (v != null) this.values.add(this.binding.toEntry(v))
            }
            this.values.sort()
        }

        override fun moveNext(): Boolean {
            if (this.boc.getAndSet(false) || !this.cursor.nextDup) {
                while (this.values.size > 0) {
                    if (this.cursor.getSearchKey(this.values.poll()) != null) {
                        return true
                    }
                }
                return false
            }
            return true
        }
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.GreaterEqual] operators.
     */
    class GreaterEqual(private val value: Value, index: BTreeIndex.Tx, columns: Array<ColumnDef<*>>): BTreeIndexCursor<ComparisonOperator.GreaterEqual>(index, columns) {
        init {
            this.cursor.getSearchKeyRange(this.binding.toEntry(this.value))
        }
        override fun moveNext(): Boolean = this.cursor.next
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate  [ComparisonOperator.GreaterEqual] operators.
     */
    class Greater(private val value: Value, index: BTreeIndex.Tx, columns: Array<ColumnDef<*>>): BTreeIndexCursor<ComparisonOperator.Greater>(index, columns) {

        init {
            val entry = this.binding.toEntry(this.value)
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
    class LessEqual(private val value: Value, index: BTreeIndex.Tx, columns: Array<ColumnDef<*>>): BTreeIndexCursor<ComparisonOperator.LessEqual>(index, columns) {
        init {
            this.cursor.getSearchKeyRange(this.binding.toEntry(this.value))
        }
        override fun moveNext(): Boolean = this.cursor.prev
    }

    /**
     * A [BTreeIndexCursor] variant to evaluate [ComparisonOperator.LessEqual] operators.
     */
    class Less(private val value: Value, index: BTreeIndex.Tx, columns: Array<ColumnDef<*>>): BTreeIndexCursor<ComparisonOperator.Less>(index, columns) {
        init {
            val entry = this.binding.toEntry(this.value)
            if (this.cursor.getSearchKeyRange(entry) != null) {
                while (this.cursor.key == entry && this.cursor.prev) {
                    /* NOOP */
                }
            }
        }

        override fun moveNext(): Boolean = this.cursor.prev
    }
}