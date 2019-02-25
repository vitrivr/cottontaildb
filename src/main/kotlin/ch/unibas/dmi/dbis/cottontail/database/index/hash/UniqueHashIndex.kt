package ch.unibas.dmi.dbis.cottontail.database.index.hash

import ch.unibas.dmi.dbis.cottontail.database.column.Column
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.database.queries.AtomicBooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.ComparisonOperator
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Recordset
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.ValidationException
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import java.nio.file.Path
import org.mapdb.Serializer
import kotlin.concurrent.read
import kotlin.concurrent.write


/**
 * Represents an index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to index one to many
 * [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return [Recordset]s.
 *
 * @see Schema
 * @see Column
 * @see Entity.Tx
 *
 * @author Ralph Gasser
 * @version 1.0f
 */
internal class UniqueHashIndex(override val name: String, override val parent: Entity) : Index() {

    /**
     * Index-wide constants.
     */
    companion object {
        const val MAP_FIELD_NAME = "map"
        const val COLUMN_FIELD_NAME = "column"
    }

    /** Path to the [UniqueHashIndex] file. */
    override val path: Path = this.parent.path.resolve("idx_$name.db")

    /** The internal [DB] reference. */
    private val db = DBMaker.fileDB(this.path.toFile()).fileMmapEnableIfSupported().transactionEnable().make()

    /**
     * Returns the list of [ColumnDef] handled by this [UniqueHashIndex]
     *
     * @return Collection of [ColumnDef].
     */
    override val columns: Array<ColumnDef<*>>
        get() = arrayOf(db.atomicString(COLUMN_FIELD_NAME).let { parent.columnForName(it.toString()) ?: throw DatabaseException.ColumnNotExistException(it.toString(), parent.name) })

    /** Map structure used for [UniqueHashIndex]. */
    private val map: HTreeMap<*, Long> = db.hashMap(MAP_FIELD_NAME, columns.first().type.serializer(columns.size), Serializer.LONG_PACKED).counterEnable().createOrOpen()

    /** The type of [Index] */
    override val type: IndexType = IndexType.HASH_UQ

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
     * @return The resulting [Recordset]
     */
    override fun lookup(predicate: Predicate): Recordset = this.txLock.read {
        /* Make necessary check. */
        if (predicate is AtomicBooleanPredicate<*>) {
            /* Create empty recordset. */
            val recordset = Recordset(this.columns)

            /* Fetch the columns that match the predicate. */
            val results = when(predicate.operator) {
                ComparisonOperator.IN -> predicate.values.map { it to this.map[it] }.toMap()
                ComparisonOperator.EQUAL -> mapOf(Pair(predicate.values.first(),this.map[predicate.values.first()]))
                else -> throw QueryException.IndexLookupFailedException(this.fqn, "Instance of unique hash-index does not support ${predicate.operator} comparison operators.")
            }

            /* Generate recordset .*/
            if (predicate.not) {
                this.map.forEach {value, tid ->
                    if (results.containsKey(value)) {
                        recordset.addRow(tupleId = tid, values = arrayOf(value))
                    }
                }
            } else {
                results.forEach {
                    if (it.value != null) {
                        recordset.addRow(tupleId = it.value!!, values = arrayOf(it.key))
                    }
                }
            }

            recordset
        } else {
            throw QueryException.IndexLookupFailedException(this.fqn, "Instance of unique hash-index does not support predicates of type '${predicate::class.simpleName}'.")
        }
    }

    /**
     * (Re-)builds the [UniqueHashIndex].
     *
     * @param columns List of columns to build the index. If null, the existing columns will be used.
     */
    override fun update(columns: Collection<ColumnDef<*>>?) = txLock.write {
        /* Store columns. */
        if (columns != null) {
            this.db.atomicString(COLUMN_FIELD_NAME).createOrOpen().set(columns.first().name)
        }

        /* Clear existing map. */
        this.map.clear()

        /* (Re-)create index entries. */
        val localMap = this.map as HTreeMap<Any,Long>
        this.parent.Tx(true).begin { tx ->
            tx.parallelForEach({
                val value = it[this.columns[0]] ?: throw ValidationException.IndexUpdateException(this.fqn, "A values cannot be null for instances of unique hash-index but tid=${it.tupleId} is")
                if (!localMap.containsKey(value)) {
                    localMap[value] = it.tupleId
                } else {
                    throw ValidationException.IndexUpdateException(this.fqn, "Values must be unique for instances of unique hash-index but '$value' (tid=${it.tupleId}) is not !")
                }
                this.db.commit()
            }, this.columns, 2)
            true
        }
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
     * Closes this [UniqueHashIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }
}