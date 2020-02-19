package ch.unibas.dmi.dbis.cottontail.database.index.va.vaplus

import ch.unibas.dmi.dbis.cottontail.database.column.Column
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.events.DataChangeEvent
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.index.IndexType
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.Predicate
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.VectorValue
import ch.unibas.dmi.dbis.cottontail.utilities.extensions.write
import ch.unibas.dmi.dbis.cottontail.utilities.name.Name
import org.apache.commons.math3.linear.MatrixUtils
import org.mapdb.Atomic
import org.mapdb.DBMaker
import org.slf4j.LoggerFactory
import java.nio.file.Path

/**
 * Represents a VAF based index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to
 * index one to many [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return
 * [Recordset]s.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class VAPlusIndex(override val name: Name, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {

    /**
     * Index-wide constants.
     */
    companion object {
        const val META_FIELD_NAME = "vaf_meta"
        const val SIGNATURE_FIELD_NAME = "vaf_signatures"
        private val LOGGER = LoggerFactory.getLogger(VAPlusIndex::class.java)
    }

    /** Constant FQN of the [Schema] object. */
    override val fqn: Name = this.parent.fqn.append(this.name)

    /** Path to the [VAPlusIndex] file. */
    override val path: Path = this.parent.path.resolve("idx_vaf_$name.db")

    /** The type of [Index] */
    override val type: IndexType = IndexType.VAF

    /** The [VAPlusIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(parent.fqn.append("distance"), ColumnType.forName("DOUBLE")))

    /** The internal [DB] reference. */
    private val db = if (parent.parent.parent.config.forceUnmapMappedFiles) {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().cleanerHackEnable().transactionEnable().make()
    } else {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().transactionEnable().make()
    }

    /** Map structure used for [VAPlusIndex]. */
    private val meta: Atomic.Var<VAPlusMeta> = this.db.atomicVar(META_FIELD_NAME, VAPlusMetaSerializer).createOrOpen()
    private val signatures = this.db.indexTreeList(SIGNATURE_FIELD_NAME, VAPlusSignatureSerializer).createOrOpen()

    /**
     * Flag indicating if this [VAPlusIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Performs a lookup through this [VAPlusIndex].
     *
     * @param predicate The [Predicate] for the lookup
     * @return The resulting [Recordset]
     */
    override fun filter(predicate: Predicate, tx: Entity.Tx): Recordset = if (predicate is KnnPredicate<*>) {
        /* Guard: Only process predicates that are supported. */
        require(this.canProcess(predicate)) { throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (vaf-index) does not support the provided predicate.") }

        /* Create empty recordset. */
        val recordset = Recordset(this.produces)
        val castPredicate = predicate as KnnPredicate<*>

        /* VAPlus. */
        val vaPlus = VAPlus()
        // TODO

        /* Generate record set .*/
        val meta = this.meta.get()

        for (i in predicate.query.indices) {
            val query = predicate.query[i].value as FloatArray
            val knn = HeapSelect<ComparablePair<Long, Double>>(castPredicate.k)
            // TODO
            vaPlus.scan(query, meta.marks)

        }

        recordset
    } else {
        throw QueryException.UnsupportedPredicateException("Index '${this.fqn}' (vaf-index) does not support predicates of type '${predicate::class.simpleName}'.")
    }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [VAPlusIndex].
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
     * Calculates the cost estimate of this [VAPlusIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate): Float = when {
        predicate !is KnnPredicate<*> || predicate.columns.first() != this.columns[0] -> Float.MAX_VALUE
        else -> 1f // TODO
    }

    /**
     * Returns true since [VAPlusIndex] supports incremental updates.
     *
     * @return True
     */
    override fun supportsIncrementalUpdate(): Boolean = true

    /**
     * (Re-)builds the [VAPlusIndex].
     */
    override fun rebuild(tx: Entity.Tx) {
        LOGGER.trace("rebuilding index {}", name)

        /* Clear existing map. */
        this.signatures.clear()

        /* VAPlus. */
        val vaPlus = VAPlus()
        val trainingSize = 1000
        val minimumNumberOfTuples = 1000
        val dimension = this.columns[0].size

        /* (Re-)create index entries. */
        var data = tx.readAll()
        // VA-file get data sample
        var dataSampleTmp = vaPlus.getDataSample(data, this.columns[0], maxOf(trainingSize, minimumNumberOfTuples))
        // VA-file in KLT domain
        var (dataSample, kltMatrix) = vaPlus.transformToKLTDomain(dataSampleTmp)
        // Non-uniform bit allocation
        val b = vaPlus.nonUniformBitAllocation(dataSample, dimension * 2)
        // Non-uniform quantization
        val (marks, signature) = vaPlus.nonUniformQuantization(dataSample, b)
        val kltMatrixBar = kltMatrix.transpose()
        data.forEach {
            val doubleArray = vaPlus.convertToDoubleArray(it[this.columns[0]] as VectorValue<*>) // TODO clean
            val dataMatrix = MatrixUtils.createRealMatrix(arrayOf(doubleArray))
            val vector = kltMatrixBar.multiply(dataMatrix.transpose()).getColumnVector(0).toArray()
            this.signatures.add(VAPlusSignature(it.tupleId, vaPlus.getSignature(vector, marks)))
        }
        val meta = VAPlusMeta(marks as Array<List<Double>>, kltMatrix)
        this.meta.set(meta)
        this.db.commit()
    }

    /**
     * Updates the [VAPlusIndex] with the provided [Record]. This method determines, whether the [Record] should be added or updated
     *
     * @param record Record to update the [VAPlusIndex] with.
     */
    override fun update(update: Collection<DataChangeEvent>, tx: Entity.Tx) = try {
        // TODO
    } catch (e: Throwable) {
        this.db.rollback()
        throw e
    }

    /**
     * Closes this [VAPlusIndex] and the associated data structures.
     */
    override fun close() = this.globalLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }

}