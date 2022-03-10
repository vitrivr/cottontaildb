package org.vitrivr.cottontail.dbms.index.hash

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TxException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.index.*
import org.vitrivr.cottontail.dbms.operations.Operation
import org.vitrivr.cottontail.storage.serializers.ValueSerializerFactory
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding
import java.util.*
import kotlin.concurrent.withLock

/**
 * Represents an index in the Cottontail DB data model, that uses a persistent [HashMap] to map a
 * unique [Value] to a [TupleId]. Well suited for equality based lookups of [Value]s.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class UniqueHashIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    /** The type of [AbstractIndex] */
    override val type: IndexType = IndexType.BTREE_UQ

    /** The [UniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** True since [UniqueHashIndex] supports incremental updates. */
    override val supportsIncrementalUpdate: Boolean = true

    /** False, since [UniqueHashIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    /** [UniqueHashIndex] does not have an [IndexConfig]*/
    override val config: IndexConfig = NoIndexConfig

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [UniqueHashIndex]. [UniqueHashIndex] can be used to process IN and EQUALS
     * comparison operations on the specified column
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = predicate is BooleanPredicate.Atomic
            && !predicate.not
            && predicate.columns.contains(this.columns[0])
            && (predicate.operator is ComparisonOperator.In || predicate.operator is ComparisonOperator.Binary.Equal)

    /**
     * Calculates the cost estimate of this [UniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost {
        if (predicate !is BooleanPredicate.Atomic || predicate.columns.first() != this.columns[0] || predicate.not) return Cost.INVALID
        return when (val operator = predicate.operator) {
            is ComparisonOperator.Binary.Equal -> Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS + Cost(memory = predicate.columns.sumOf { it.type.physicalSize }.toFloat())
            is ComparisonOperator.In -> (Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS) * operator.right.size + Cost(memory = predicate.columns.sumOf { it.type.physicalSize }.toFloat())
            else -> Cost.INVALID
        }
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [AbstractIndex].
     *
     * @param context [TransactionContext] to open the [AbstractIndex.Tx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [UniqueHashIndex]
     */
    override fun close() {
        /* No op. */
    }

    /**
     * An [IndexTx] that affects this [UniqueHashIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The internal [XodusBinding] reference used for de-/serialization. */
        private val binding: XodusBinding<*> = ValueSerializerFactory.xodus(this.columns[0].type, this.columns[0].nullable)

        /** [UniqueHashIndex] does not have an [IndexConfig]*/
        override val config: IndexConfig
            get() = this@UniqueHashIndex.config

        /**
         * Adds a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to add a mapping for.
         * @param tupleId The [TupleId] for the mapping.
         *
         * This is an internal function and can be used safely with values o
         */
        @Suppress("UNCHECKED_CAST")
        private fun addMapping(key: Value, tupleId: TupleId): Boolean {
            val keyRaw = (this.binding as XodusBinding<Value>).valueToEntry(key)
            val tupleIdRaw = LongBinding.longToCompressedEntry(tupleId)
            return if (this.dataStore.get(this.context.xodusTx, keyRaw) != null) {
                this.dataStore.put(this.context.xodusTx, keyRaw, tupleIdRaw)
            } else {
                false
            }
        }

        /**
         * Removes a mapping from the given [Value] to the given [TupleId].
         *
         * @param key The [Value] key to remove a mapping for.
         *
         * This is an internal function and can be used safely with values o
         */
        @Suppress("UNCHECKED_CAST")
        private fun removeMapping(key: Value): Boolean {
            val keyRaw = (this.binding as XodusBinding<Value>).valueToEntry(key)
            return this.dataStore.delete(this.context.xodusTx, keyRaw)
        }

        /**
         * (Re-)builds the [UniqueHashIndex].
         */
        override fun rebuild() = this.txLatch.withLock {
            /* Obtain Tx for parent [Entity. */
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Truncate, reopen and repopulate store. */
            this.clear()
            entityTx.cursor(this@UniqueHashIndex.columns).forEach { record ->
                val value = record[this.dbo.columns[0]] ?: throw TxException.TxValidationException(this.context.txId, "Value cannot be null for UniqueHashIndex ${this@UniqueHashIndex.name} given value is (value = null, tupleId = ${record.tupleId}).")
                if (!this.addMapping(value, record.tupleId)) {
                    throw TxException.TxValidationException(this.context.txId, "Value must be unique for UniqueHashIndex ${this@UniqueHashIndex.name} but is not (value = $value, tupleId = ${record.tupleId}).")
                }
            }
        }

        /**
         * Updates the [UniqueHashIndex] with the provided [Operation.DataManagementOperation.InsertOperation]
         *
         * @param operation [Operation.DataManagementOperation.InsertOperation]s to process.
         */
        override fun insert(operation: Operation.DataManagementOperation.InsertOperation) = this.txLatch.withLock {
            val value = operation.inserts[this.dbo.columns[0]]
            if (value != null) {
                this.addMapping(value, operation.tupleId)
            }
        }

        /**
         * Updates the [UniqueHashIndex] with the provided [Operation.DataManagementOperation.UpdateOperation]s.
         *
         * @param operation [Operation.DataManagementOperation.UpdateOperation]s to process.
         */
        override fun update(operation: Operation.DataManagementOperation.UpdateOperation) = this.txLatch.withLock {
            val old = operation.updates[this.dbo.columns[0]]?.first
            if (old != null) {
                this.removeMapping(old)
            }
            val new = operation.updates[this.dbo.columns[0]]?.second
            if (new != null) {
                this.addMapping(new, operation.tupleId)
            }
        }

        /**
         * Updates the [UniqueHashIndex] with the provided [Operation.DataManagementOperation.DeleteOperation]s.
         *
         * @param operation [Operation.DataManagementOperation.DeleteOperation]s to apply.
         */
        override fun delete(operation: Operation.DataManagementOperation.DeleteOperation) = this.txLatch.withLock {
            val old = operation.deleted[this.dbo.columns[0]]
            if (old != null) {
                this.removeMapping(old)
            }
        }

        /**
         * Clears the [UniqueHashIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.txLatch.withLock {
            this@UniqueHashIndex.parent.parent.parent.environment.truncateStore(this@UniqueHashIndex.name.storeName(), this.context.xodusTx)
            this.dataStore = this@UniqueHashIndex.parent.parent.parent.environment.openStore(
                this@UniqueHashIndex.name.storeName(),
                StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING,
                this.context.xodusTx,
                false
            ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${this@UniqueHashIndex.name} is missing.")
        }

        /**
         * Performs a lookup through this [UniqueHashIndex.Tx] and returns a [Cursor] of all [Record]s that match the [Predicate].
         * Only supports [BooleanPredicate.Atomic]s.
         *
         * The [Cursor] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Cursor]
         */
        override fun filter(predicate: Predicate) = object : Cursor<Record> {

            /** Local [BooleanPredicate.Atomic] instance. */
            private val predicate: BooleanPredicate.Atomic

            /** A [Queue] with values that should be queried. */
            private val queryValueQueue: Queue<Value> = LinkedList()

            /** The current query [Value]. */
            private var queryValue: Value

            /** Internal cursor used for navigation. */
            private var cursor: jetbrains.exodus.env.Cursor

            /* Perform initial sanity checks. */
            init {
                require(predicate is BooleanPredicate.Atomic) { "UniqueHashIndex.filter() does only support Atomic.Literal boolean predicates." }
                require(!predicate.not) { "UniqueHashIndex.filter() does not support negated statements (i.e. NOT EQUALS or NOT IN)." }
                this.predicate = predicate
                when (predicate.operator) {
                    is ComparisonOperator.In -> this.queryValueQueue.addAll((predicate.operator as ComparisonOperator.In).right.mapNotNull { it.value })
                    is ComparisonOperator.Binary.Equal -> this.queryValueQueue.add((predicate.operator as ComparisonOperator.Binary.Equal).right.value ?: throw IllegalArgumentException("UniqueHashIndex.filter() does not support NULL operands."))
                    else -> throw IllegalArgumentException("UniqueHashIndex.filter() does only support EQUAL and IN operators.")
                }

                /** Initialize cursor. */
                this.cursor = this@Tx.dataStore.openCursor(this@Tx.context.xodusTx)
                this.queryValue = this.queryValueQueue.poll() ?: throw IllegalArgumentException("UniqueHashIndex.filter() does not support NULL operands.")
                this.cursor.getSearchKey(StringBinding.BINDING.objectToEntry(this.queryValue))
            }

            override fun moveNext(): Boolean {
                this.queryValue = this.queryValueQueue.poll() ?: return false
                return this.cursor.getSearchKey(StringBinding.BINDING.objectToEntry(this.queryValue)) != null
            }

            override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.key)

            override fun value(): Record = StandaloneRecord(this.key(), this@UniqueHashIndex.produces, arrayOf(this@Tx.binding.entryToValue(this.cursor.value)))

            override fun close() = this.cursor.close()
        }

        /**
         * The [UniqueHashIndex] does not support ranged filtering!
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Cursor].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Cursor<Record> {
            throw UnsupportedOperationException("The UniqueHashIndex does not support ranged filtering!")
        }
    }
}
