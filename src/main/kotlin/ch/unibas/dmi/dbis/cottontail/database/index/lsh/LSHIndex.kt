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
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.Value
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import org.mapdb.DBMaker
import org.mapdb.HTreeMap
import org.mapdb.Serializer
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.*

/**
 * Represents a LSH based index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to
 * index one to many [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return
 * [Recordset]s. The [LSHIndex] allows for ... TODO
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class LSHIndex(override val name: Name, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {

    /**
     * Index-wide constants.
     */
    companion object {
        const val MAP_FIELD_NAME = "lsh_map"
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
    private val map: HTreeMap<out Value<out Any>, LongArray> = this.db.hashMap(MAP_FIELD_NAME, this.columns.first().type.serializer(this.columns.size), Serializer.LONG_ARRAY).counterEnable().createOrOpen()

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
        val stages = 5 // stages of computation (iterations) LSH performs
        val buckets = 10 // number of buckets
        val dimension = 20 // dimension, R^n
        val seed = 123456 // initial seed
        val lsh = LSH(stages, buckets, dimension, seed)

        /* (Re-)create index entries. */
        val localMap = mutableMapOf<Value<*>, MutableList<Long>>()
        this.parent.Tx(readonly = true, columns = this.columns, ommitIndex = true).begin { tx ->
            tx.forEach {
                // TODO
            }
            this.db.commit()
            true
        }

        // dummy
        val dict = arrayListOf(
                doubleArrayOf(-0.01056019, -0.008349786, -0.16701701, 0.004941992, -0.01325078, 0.070494831, -0.02364802, -0.01885709, 0.028819122, 0.00508853, -0.02240928, 0.0192304977, -0.00517446, 0.035751863, 0.013102544, 0.015905944, 0.021952597, -0.03055140, 0.0213935271, -0.03402102),
                doubleArrayOf(0.031085418, 0.0084585258, 0.028911666, 0.059291279, 0.066112262, 0.034841911, -0.04517528, -0.02407402, 0.004078748, 0.03692695, 0.038350401, -0.024678318, 0.066937601, 0.000313272, -0.00575445, -0.01872067, 0.014806176, 0.017861118, 0.0452924055, -0.00087552),
                doubleArrayOf(-0.00058009, 0.3119143047, -0.13326713, 0.009902472, -0.01967259, 0.074465533, -0.00838086, -0.03009507, 0.024950046, -0.0008931, -0.01463778, 0.0120091511, -0.00973349, 0.040033633, 0.015653935, 0.021504355, 0.021574510, -0.03517487, 0.0148424692, -0.03782969),
                doubleArrayOf(0.000032882, -0.026796976, 0.024857667, 0.031494623, 0.027781745, 0.025808908, 0.034065845, 0.014800912, -0.00894230, 0.01173742, 0.018344367, 0.0009074789, 0.016907429, -0.01555382, -0.04054928, -0.00743121, -0.02440050, 0.014558027, 0.0312259357, -0.00390538),
                doubleArrayOf(-0.01864881, 0.0165908443, 0.006201774, 0.023754715, 0.086972942, 0.008385858, 0.018910873, -0.00495731, 0.015178515, 0.02431061, 0.013565008, 0.0090042693, 0.054457597, -0.00357033, -0.01540450, 0.029744557, -0.01283717, 0.024781844, 0.0529232746, 0.005411035),
                doubleArrayOf(-0.03827083, -0.031210481, -0.02854668, -0.02388333, 0.029384231, -0.03558728, -0.04363631, -0.03525492, -0.02126351, 0.02449250, 0.004537241, -0.011647115, 0.024647950, -0.01783799, -0.01955148, -0.02217470, -0.03077020, 0.009255070, -0.007289458, 0.603033921),
                doubleArrayOf(0.039896054, 0.5031662732, 0.137287591, -0.02272921, -0.02782616, 0.040105838, -0.04654621, -0.03450930, -0.01943290, 0.02535425, 0.003848669, -0.014029106, 0.102874451, -0.01783618, -0.01832342, -0.02423671, -0.03225029, 0.009499067, -0.008718754, 0.103263536),
                doubleArrayOf(-0.54356059, -0.323266232, -0.02758829, -0.75272921, 0.767826114, -0.74330538, 0.746848621, -0.73416930, -0.79943290, 0.72835425, 0.703848669, -0.714029103, 0.782654151, -0.71625618, -0.79635842, -0.72143571, -0.73997429, 0.701352067, -0.708796354, 0.715379036),
                doubleArrayOf(0.839896055, 0.8631866232, 0.897245914, -0.82224861, -0.82782616, 0.847710538, -0.88954621, -0.87860930, -0.81943290, 0.82535425, 0.803848669, -0.814679302, 0.802874451, -0.81783618, -0.81832342, -0.82423671, -0.83225029, 0.809499067, -0.808718754, 0.813263536),
                doubleArrayOf(-0.95424656, -0.983166232, -0.95678591, -0.92272921, 0.999526118, -0.94010135, -0.95654621, -0.91245930, -0.91943290, 0.92535425, 0.903848669, -0.913578607, 0.902812351, -0.93567899, -0.91832342, -0.96532371, -0.92345629, 0.765319067, -0.989648754, 0.924673536)
        )
        for (i in dict.indices) {
            val hash: IntArray = lsh.hash(dict[i])
            print(hash.contentToString())
            print("\n")
        }
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