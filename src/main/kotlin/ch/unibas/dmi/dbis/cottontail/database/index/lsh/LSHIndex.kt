package ch.unibas.dmi.dbis.cottontail.database.index.lsh

import ch.unibas.dmi.dbis.cottontail.database.column.Column
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.events.DataChangeEvent
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.exceptions.ValidationException
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.slf4j.LoggerFactory
import java.nio.file.Path

/**
 * Represents a LSH based index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to
 * index one to many [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return
 * [Recordset]s. The [LSHIndex] allows for ... TODO
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class LSHIndex(override val name: Name, override val parent: Entity, override val columns: Array<ColumnDef<*>>, val params: Map<String, String> = emptyMap()) : Index() {

    /**
     * Index-wide constants.
     */
    companion object {
        const val MAP_FIELD_NAME = "lsh_map"
        const val MAP_FIELD_NAME_CONFIG = "lsh_map_config"
        private val LOGGER = LoggerFactory.getLogger(LSHIndex::class.java)
    }

    /** Constant FQN of the [Schema] object. */
    override val fqn: Name = this.parent.fqn.append(this.name)

    /** Path to the [LSHIndex] file. */
    override val path: Path = this.parent.path.resolve("idx_lsh_$name.db")

    /** The type of [Index] */
    override val type: IndexType = IndexType.LSH

    /** The [LSHIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = this.columns

    /** The internal [DB] reference. */
    private val db = if (parent.parent.parent.config.forceUnmapMappedFiles) {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().cleanerHackEnable().transactionEnable().make()
    } else {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().transactionEnable().make()
    }

    /** Map structure used for [LSHIndex]. */
    private val map: HTreeMap<Int, LongArray> = this.db.hashMap(MAP_FIELD_NAME, Serializer.INTEGER, Serializer.LONG_ARRAY).counterEnable().createOrOpen()

    /** Map config used for parameters of [LSHIndex]. */
    private val config: HTreeMap<String, Int> = this.db.hashMap(MAP_FIELD_NAME_CONFIG, Serializer.STRING, Serializer.INTEGER).counterEnable().createOrOpen()

    /**
     * Flag indicating if this [LSHIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Performs a lookup through this [LSHIndex].
     *
     * @param predicate The [Predicate] for the lookup
     * @return The resulting [Recordset]
     */
    override fun filter(predicate: Predicate): Recordset = if (predicate is KnnPredicate<*>) {
        /* Create empty recordset. */
        val recordset = Recordset(this.columns)

        // TODO Gibt Recordset mit tupleIds und (ggf.) Distanzen zurück
        println("filter called")

        recordset
    } else {
        throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (lsh-index) does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [LSHIndex].
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate): Boolean = if (predicate is KnnPredicate<*>) {
        // TODO
        predicate.columns.first() == this.columns[0]
    } else {
        false
    }

    /**
     * Calculates the cost estimate of this [LSHIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Float = when {
        // TODO
        predicate !is KnnPredicate<*> || predicate.columns.first() != this.columns[0] -> Float.MAX_VALUE
        else -> Float.MAX_VALUE
    }

    /**
     * Returns true since [LSHIndex] supports incremental updates.
     *
     * @return True
     */
    override fun supportsIncrementalUpdate(): Boolean = true

    /**
     * (Re-)builds the [LSHIndex].
     */
    override fun rebuild() {
        LOGGER.trace("rebuilding index {}", name)

        /* Clear existing map. */
        this.map.clear()

        /* LSH. */
        params["stages"]?.let { this.config["stages"] = it.toInt() }
        params["buckets"]?.let { this.config["buckets"] = it.toInt() }
        params["seed"]?.let { this.config["seed"] = it.toInt() }
        val lsh = LSH(this.config["stages"]!!, this.config["buckets"]!!, this.columns[0].size, this.config["seed"]!!)

        /* (Re-)create index entries. */
        val localMap = mutableMapOf<Int, MutableList<Long>>()
        this.parent.Tx(readonly = true, columns = this.columns, ommitIndex = true).begin { tx ->
            tx.forEach {
                val value = it[this.columns[0]]
                        ?: throw ValidationException.IndexUpdateException(this.fqn, "A value cannot be null for instances of lsh-index but tid=${it.tupleId} is")
                val bucket: Int = lsh.hash(value.value as FloatArray)!!.last() // bucket after s stages
                if (!localMap.containsKey(bucket)) {
                    localMap[bucket] = mutableListOf(it.tupleId)
                } else {
                    localMap[bucket]!!.add(it.tupleId)
                }
            }
            val castMap = this.map
            localMap.forEach { (bucket, l) -> castMap[bucket] = l.toLongArray() }
            this.db.commit()
            true
        }
        // debug this.map.forEach { (key: Int, value: LongArray) -> println("$key: " + value.size) }
    }

    /**
     * Updates the [LSHIndex] with the provided [Record]. This method determines, whether the [Record] should be added or updated
     *
     * @param record Record to update the [LSHIndex] with.
     */
    override fun update(update: Collection<DataChangeEvent>) = try {
        // TODO
    } catch (e: Throwable) {
        this.db.rollback()
        throw e
    }

    /**
     * Closes this [LSHIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }

}