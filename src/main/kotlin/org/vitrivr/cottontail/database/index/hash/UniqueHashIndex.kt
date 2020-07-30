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
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.components.AtomicBooleanPredicate
import org.vitrivr.cottontail.database.queries.components.ComparisonOperator
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.exceptions.ValidationException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.utilities.extensions.write
import org.vitrivr.cottontail.utilities.name.Name
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
 * @version 1.1
 */
class UniqueHashIndex(override val name: Name, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {

    /**
     * Index-wide constants.
     */
    companion object {
        const val MAP_FIELD_NAME = "map"
        private val LOGGER = LoggerFactory.getLogger(UniqueHashIndex::class.java)
    }

    /** Constant FQN of the [Schema] object. */
    override val fqn: Name = this.parent.fqn.append(this.name)

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
     * Performs a lookup through this [UniqueHashIndex].
     *
     * @param predicate The [Predicate] for the lookup
     * @param tx Reference to the [Entity.Tx] the call to this method belongs to.
     * @return The resulting [Recordset]
     */
    override fun filter(predicate: Predicate, tx: Entity.Tx): Recordset = if (predicate is AtomicBooleanPredicate<*>) {
        /* Create empty recordset. */
        val recordset = Recordset(this.columns)

        /* Fetch the columns that match the predicate. */
        val results = when (predicate.operator) {
            ComparisonOperator.IN -> predicate.values.map { it to this.map[it] }.toMap()
            ComparisonOperator.EQUAL -> mapOf(Pair(predicate.values.first(), this.map[predicate.values.first()]))
            else -> throw QueryException.IndexLookupFailedException(this.fqn, "Instance of unique hash-index does not support ${predicate.operator} comparison operators.")
        }

        /* Generate record set .*/
        if (predicate.not) {
            this.map.forEach { (value, tid) ->
                if (results.containsKey(value)) {
                    recordset.addRowUnsafe(tupleId = tid, values = arrayOf(value))
                }
            }
        } else {
            results.forEach {
                if (it.value != null) {
                    recordset.addRowUnsafe(tupleId = it.value!!, values = arrayOf(it.key))
                }
            }
        }
        recordset
    } else {
        throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (unique hash-index) does not support predicates of type '${predicate::class.simpleName}'.")
    }

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
     * (Re-)builds the [UniqueHashIndex].
     *
     * @param tx Reference to the [Entity.Tx] the call to this method belongs to.
     */
    override fun rebuild(tx: Entity.Tx) {
        LOGGER.trace("Rebuilding index {}", name)

        /* Clear existing map. */
        this.map.clear()

        /* (Re-)create index entries. */
        val localMap = this.map as HTreeMap<Value, Long>
        tx.forEach {
            //TODO: Check how cases should be handled: 1) null values 2) non-unique values.
            val value = it[this.columns[0]]
                    ?: throw ValidationException.IndexUpdateException(this.fqn, "A value cannot be null for instances of unique hash-index but tid=${it.tupleId} is.")
            if (!localMap.containsKey(value)) {
                localMap[value] = it.tupleId
            } else {
                LOGGER.warn("Value must be unique for instances of unique hash-index but '$value' (tid=${it.tupleId}) is not! Skipping entry...")
            }
        }
        this.db.commit()
    }

    /**
     * Updates the [UniqueHashIndex] with the provided [DataChangeEvent]s.
     *
     * @param update [DataChangeEvent]s based on which to update the [UniqueHashIndex].
     */
    override fun update(update: Collection<DataChangeEvent>, tx: Entity.Tx) = try {
        val localMap = this.map as HTreeMap<Value,Long>

        /* Define action for inserting an entry based on a DataChangeEvent. */
        val atomicInsert = { event: DataChangeEvent ->
            val newValue = event.new?.get(this.columns[0])
                    ?: throw ValidationException.IndexUpdateException(this.fqn, "Values cannot be null for instances of UniqueHashIndex but tid=${event.new?.tupleId} is.")
            localMap[newValue] = event.new.tupleId
        }

        /* Define action for deleting an entry based on a DataChangeEvent. */
        val atomicDelete = { event: DataChangeEvent ->
            val oldValue = event.old?.get(this.columns[0])
                    ?: throw ValidationException.IndexUpdateException(this.fqn, "Values cannot be null for instances of UniqueHashIndex but tid=${event.new?.tupleId} is.")
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
        this.db.commit()
    } catch (e: Throwable) {
        this.db.rollback()
        throw e
    }

    /**
     * Closes this [UniqueHashIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }
}
