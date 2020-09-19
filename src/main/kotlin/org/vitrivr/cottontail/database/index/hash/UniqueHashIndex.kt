package org.vitrivr.cottontail.database.index.hash

import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.events.DataChangeEventType
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTransaction
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.components.AtomicBooleanPredicate
import org.vitrivr.cottontail.database.queries.components.ComparisonOperator
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.exceptions.ValidationException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path

/**
 * Represents an index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to index one to many
 * [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return [Recordset]s.
 *
 * @see Schema
 * @see Column
 * @see Entity.Tx
 *
 * @author Ralph Gasser
 * @version 1.2
 */
class UniqueHashIndex(override val name: Name.IndexName, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {

    /**
     * Index-wide constants.
     */
    companion object {
        const val MAP_FIELD_NAME = "map"
        private val LOGGER = LoggerFactory.getLogger(UniqueHashIndex::class.java)
    }

    /** Path to the [UniqueHashIndex] file. */
    override val path: Path = this.parent.path.resolve("idx_$name.db")

    /** The type of [Index] */
    override val type: IndexType = IndexType.HASH_UQ

    /** The [UniqueHashIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** The internal [DB] reference. */
    private val db = if (parent.parent.parent.config.memoryConfig.forceUnmapMappedFiles) {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().transactionEnable().make()
    } else {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().transactionEnable().make()
    }

    /** Map structure used for [UniqueHashIndex]. */
    private val map: HTreeMap<out Value, Long> = this.db.hashMap(MAP_FIELD_NAME, this.columns.first().type.serializer(this.columns.size), Serializer.LONG_PACKED).counterEnable().createOrOpen()

    /**
     * Flag indicating if this [UniqueHashIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [UniqueHashIndex]. [UniqueHashIndex] can be used to process IN and EQUALS
     * comparison operations on the specified column
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = if (predicate is AtomicBooleanPredicate<*>) {
        predicate.columns.first() == this.columns[0] && (predicate.operator == ComparisonOperator.IN || predicate.operator == ComparisonOperator.EQUAL)
    } else {
        false
    }

    /**
     * Calculates the cost estimate of this [UniqueHashIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Cost = when {
        predicate !is AtomicBooleanPredicate<*> || predicate.columns.first() != this.columns[0] -> Cost.INVALID
        predicate.operator == ComparisonOperator.EQUAL -> Cost(Costs.DISK_ACCESS_READ, Costs.MEMORY_ACCESS_READ, predicate.columns.map { it.physicalSize }.sum().toFloat())
        predicate.operator == ComparisonOperator.IN -> Cost(Costs.DISK_ACCESS_READ, Costs.MEMORY_ACCESS_READ, predicate.columns.map { it.physicalSize }.sum().toFloat()) * predicate.values.size
        else -> Cost.INVALID
    }

    /**
     * Returns true since [UniqueHashIndex] supports incremental updates.
     *
     * @return True
     */
    override fun supportsIncrementalUpdate(): Boolean = true

    /**
     * Opens and returns a new [IndexTransaction] object that can be used to interact with this [Index].
     *
     * @param parent If the [Entity.Tx] that requested the [IndexTransaction].
     */
    override fun begin(parent: Entity.Tx): IndexTransaction = Tx(parent)

    /**
     * Closes this [UniqueHashIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }

    /**
     * A [IndexTransaction] that affects this [UniqueHashIndex].
     */
    private inner class Tx(parent: Entity.Tx) : Index.Tx(parent) {
        /**
         * (Re-)builds the [UniqueHashIndex].
         **/
        override fun rebuild() = this.localLock.read {
            /* Check if this [Tx] allowed to write. */
            checkValidForWrite()

            /* Clear existing map. */
            this@UniqueHashIndex.map.clear()

            /* (Re-)create index entries. */
            val localMap = this@UniqueHashIndex.map as HTreeMap<Value, Long>
            this.parent.scan().forEach { tid ->
                val record = this.parent.read(tid)
                val value = record[this.columns[0]]
                        ?: throw ValidationException.IndexUpdateException(this.name, "A value cannot be null for instances of non-unique hash-index but tid=$tid is")
                if (!localMap.containsKey(value)) {
                    localMap[value] = tid
                } else {
                    LOGGER.warn("Value must be unique for instances of unique hash-index but '$value' (tid=$tid) is not! Skipping entry...")
                }
            }
            this@UniqueHashIndex.db.commit()
        }

        /**
         * Updates the [UniqueHashIndex] with the provided [DataChangeEvent]s. This method determines,
         * whether the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param update Collection of [DataChangeEvent]s to process.
         */
        override fun update(update: Collection<DataChangeEvent>) = this.localLock.read {
            try {
                /* Check if this [Tx] allowed to write. */
                checkValidForWrite()
                val localMap = this@UniqueHashIndex.map as HTreeMap<Value, Long>

                /* Define action for inserting an entry based on a DataChangeEvent. */
                val atomicInsert = { event: DataChangeEvent ->
                    val newValue = event.new?.get(this.columns[0])
                            ?: throw ValidationException.IndexUpdateException(this.name, "Values cannot be null for instances of UniqueHashIndex but tid=${event.new?.tupleId} is.")
                    localMap[newValue] = event.new.tupleId
                }

                /* Define action for deleting an entry based on a DataChangeEvent. */
                val atomicDelete = { event: DataChangeEvent ->
                    val oldValue = event.old?.get(this.columns[0])
                            ?: throw ValidationException.IndexUpdateException(this.name, "Values cannot be null for instances of UniqueHashIndex but tid=${event.new?.tupleId} is.")
                    localMap.remove(oldValue)
                }

                /* Process the DataChangeEvents. */
                loop@ for (event in update) {
                    when (event.type) {
                        DataChangeEventType.INSERT -> atomicInsert(event)
                        DataChangeEventType.UPDATE -> {
                            if (event.new?.get(this.columns[0]) != event.old?.get(this.columns[0])) {
                                atomicDelete(event)
                                atomicInsert(event)
                            }
                        }
                        DataChangeEventType.DELETE -> atomicDelete(event)
                        else -> continue@loop
                    }
                }

                /* Commit the change. */
                this@UniqueHashIndex.db.commit()
            } catch (e: Throwable) {
                this@UniqueHashIndex.db.rollback()
                throw e
            }
        }

        /**
         * Performs a lookup through this [UniqueHashIndex.Tx] and returns a [CloseableIterator] of
         * all [TupleId]s that match the [Predicate]. Only supports [AtomicBooleanPredicate]s.
         *
         * The [CloseableIterator] is not thread safe!
         *
         * <strong>Important:</strong> It remains to the caller to close the [CloseableIterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @param tx Reference to the [Entity.Tx] the call to this method belongs to.
         *
         * @return The resulting [CloseableIterator]
         */
        override fun filter(predicate: Predicate): CloseableIterator<TupleId> = object : CloseableIterator<TupleId> {

            /** Cast [AtomicBooleanPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate !is AtomicBooleanPredicate<*>) {
                throw QueryException.UnsupportedPredicateException("Index '${this@UniqueHashIndex.name}' (non-unique hash-index) does not support predicates of type '${predicate::class.simpleName}'.")
            } else {
                predicate
            }

            /* Perform some sanity checks. */
            init {
                checkValidForRead()
            }

            /** Generates a shared lock on the enclosing [Tx]. This lock is kept until the [CloseableIterator] is closed. */
            private val lock = this@Tx.localLock.readLock()

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            private var closed = false

            /** Pre-fetched [TupleId]s that match the [Predicate]. */
            private val results = if (this.predicate.not) {
                val blackList = when (this.predicate.operator) {
                    ComparisonOperator.IN,
                    ComparisonOperator.EQUAL -> this.predicate.values.mapNotNull { this@UniqueHashIndex.map[it] }.toMutableList()
                    else -> throw QueryException.UnsupportedPredicateException("Instance of unique hash-index does not support ${this.predicate.operator} comparison operators.")
                }
                this@UniqueHashIndex.map.values.filterNotNull().filter { !blackList.contains(it) }.toMutableList()
            } else {
                when (this.predicate.operator) {
                    ComparisonOperator.IN,
                    ComparisonOperator.EQUAL -> this.predicate.values.mapNotNull { this@UniqueHashIndex.map[it] }.toMutableList()
                    else -> throw QueryException.UnsupportedPredicateException("Instance of unique hash-index does not support ${this.predicate.operator} comparison operators.")
                }
            }

            /**
             * Returns `true` if the iteration has more elements.
             */
            override fun hasNext(): Boolean {
                check(!this.closed) { "Illegal invocation of hasNext(): This CloseableIterator has been closed." }
                return this.results.isNotEmpty()
            }

            /**
             * Returns the next element in the iteration.
             */
            override fun next(): TupleId {
                check(!this.closed) { "Illegal invocation of next(): This CloseableIterator has been closed." }
                return this.results.removeFirst()
            }

            /**
             * Closes this [CloseableIterator] and releases all locks and resources associated with it.
             */
            override fun close() {
                if (!this.closed) {
                    this@Tx.localLock.unlock(this.lock)
                    this.closed = true
                }
            }
        }
    }
}
