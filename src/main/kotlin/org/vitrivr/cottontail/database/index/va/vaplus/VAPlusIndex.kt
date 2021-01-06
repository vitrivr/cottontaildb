package org.vitrivr.cottontail.database.index.va.vaplus

import org.apache.commons.math3.linear.MatrixUtils
import org.mapdb.Atomic
import org.mapdb.DBMaker
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.va.SignatureGenerator
import org.vitrivr.cottontail.database.index.va.VectorApproximationSignature
import org.vitrivr.cottontail.database.index.va.VectorApproximationSignatureSerializer
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path

/**
 * Represents a VAF based index in the Cottontail DB data model. An [Index] belongs to an [Entity] and can be used to
 * index one to many [Column]s. Usually, [Index]es allow for faster data access. They process [Predicate]s and return
 * [Recordset]s.
 *
 * TODO: Fix and finalize implementation.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class VAPlusIndex(override val name: Name.IndexName, override val parent: Entity, override val columns: Array<ColumnDef<*>>) : Index() {

    /**
     * Index-wide constants.
     */
    companion object {
        const val META_FIELD_NAME = "vapf_meta"
        const val SIGNATURE_FIELD_NAME = "vapf_signatures"
        private val LOGGER = LoggerFactory.getLogger(VAPlusIndex::class.java)
    }

    /** Path to the [VAPlusIndex] file. */
    override val path: Path = this.parent.path.resolve("idx_vapf_$name.db")

    /** The type of [Index] */
    override val type: IndexType = IndexType.VAF

    /** The [VAPlusIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.parent.name.column("distance"), ColumnType.forName("DOUBLE")))

    /** The internal [DB] reference. */
    private val db = if (parent.parent.parent.config.memoryConfig.forceUnmapMappedFiles) {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().cleanerHackEnable().transactionEnable().make()
    } else {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().transactionEnable().make()
    }

    /** Map structure used for [VAPlusIndex]. */
    private val meta: Atomic.Var<VAPlusMeta> = this.db.atomicVar(META_FIELD_NAME, VAPlusMetaSerializer).createOrOpen()
    private val signatures = this.db.indexTreeList(SIGNATURE_FIELD_NAME, VectorApproximationSignatureSerializer).createOrOpen()

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
        require(this.canProcess(predicate)) { throw QueryException.UnsupportedPredicateException("Index '${this.name}' (vaf-index) does not support the provided predicate.") }

        /* Create empty recordset. */
        val recordset = Recordset(this.produces)

        /* VAPlus. */
        val vaPlus = VAPlus()

        /* Extract meta record with all the helper classes. .*/
        val meta = this.meta.get()

        /* Prepare HeapSelect data structures for Phase 1KNN query.*/
        val heapsP1 = Array<MinHeapSelection<ComparablePair<Pair<Long, DoubleValue>, DoubleValue>>>(predicate.query.size) {
            MinHeapSelection(5 * predicate.k)
        }

        /* Prepare HeapSelect data structures for Phase 2 of KNN query.*/
        val heapsP2 = Array<MinHeapSelection<ComparablePair<Long, DoubleValue>>>(predicate.query.size) {
            MinHeapSelection(predicate.k)
        }

        /* Prepare empty IntArray for bounds estimation. */
        val d = IntArray(predicate.query.size) {
            Int.MAX_VALUE
        }

        /* Calculate the relevant bounds per query. */
        val queryBounds = predicate.query.map {
            /* Transform into KLT domain. */
            val dataMatrix = MatrixUtils.createRealMatrix(arrayOf(vaPlus.convertToDoubleArray(it)))
            val vector = meta.kltMatrix.multiply(dataMatrix.transpose()).getColumnVector(0).toArray()

            val bounds = vaPlus.computeBounds(vector, meta.marks)
            // what for?? why do we need bounds for query? we have it exactly... we need to get bounds
            // of data vectors and from there estimate bounds on IP or L2...
            val (lbIndex, lbBounds) = vaPlus.compressBounds(bounds.first)
            val (ubIndex, ubBounds) = vaPlus.compressBounds(bounds.second)

            Pair(Pair(lbIndex, lbBounds), Pair(ubIndex, ubBounds))
        }

        /* Phase 1: Iterate over signatures, re-construct cells and calculate lower and upper bound. */
        this.signatures.forEach {
            queryBounds.forEachIndexed { i, query ->
                //val cells = meta.signatureGenerator.toCells(it!!.signature.toString())
                val cells = it!!.signature
                val lb = DoubleValue(this.calculateBounds(cells, query.first.second, query.first.first))
                val ub = DoubleValue(this.calculateBounds(cells, query.second.second, query.second.first))
                if (heapsP1[i].size < heapsP1[i].k) {
                    heapsP1[i].offer(ComparablePair(Pair(it.tupleId, ub), lb))
                } else if (lb < heapsP1[i][heapsP1[i].k - 1].first.second) {
                    heapsP1[i].offer(ComparablePair(Pair(it.tupleId, ub), lb))
                }
            }
        }

        /* Phase 2: Iterate over filtered list to calculate final results. */
        heapsP1.forEachIndexed { i, it ->
            (0 until it.size).forEach { j ->
                if (heapsP2[i].size < heapsP2[i].k) {
                    val vector = tx.read(it[j].first.first)[predicate.column]
                    heapsP2[i].offer(ComparablePair(it[j].first.first, (predicate as KnnPredicate<VectorValue<*>>).distance(predicate.query[i], vector as VectorValue<*>)))
                } else {
                    if (it[j].second < heapsP2[i][heapsP2[i].k - 1].second) {
                        return@forEach
                    }
                    val vector = tx.read(it[j].first.first)[predicate.column]
                    heapsP2[i].offer(ComparablePair(it[j].first.first, (predicate as KnnPredicate<VectorValue<*>>).distance(predicate.query[i], vector as VectorValue<*>)))
                }
            }
        }

        /* Add results to recordset. */
        for (heap in heapsP2) {
            for (j in 0 until heap.size) {
                recordset.addRowUnsafe(heap[j].first, arrayOf(DoubleValue(heap[j].second)))
            }
        }
        recordset
    } else {
        throw QueryException.UnsupportedPredicateException("Index '${this.name}' (vaf-index) does not support predicates of type '${predicate::class.simpleName}'.")
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
    override fun cost(predicate: Predicate): Cost = when {
        predicate !is KnnPredicate<*> || predicate.columns.first() != this.columns[0] -> Cost.INVALID
        else -> Cost.ZERO /* TODO: Determine. */
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
        val dimension = this.columns[0].logicalSize

        // VA-file get data sample
        val dataSampleTmp = vaPlus.getDataSample(tx, this.columns[0], maxOf(trainingSize, minimumNumberOfTuples))

        // VA-file in KLT domain
        val (dataSample, kltMatrix) = vaPlus.transformToKLTDomain(dataSampleTmp)

        // Non-uniform bit allocation
        val b = vaPlus.nonUniformBitAllocation(dataSample, dimension * 2)
        val signatureGenerator = SignatureGenerator(b)

        // Non-uniform quantization
        val marks = vaPlus.nonUniformQuantization(dataSample, b)

        // Indexing
        val kltMatrixBar = kltMatrix.transpose()
        tx.forEach {
            val doubleArray = vaPlus.convertToDoubleArray(it[this.columns[0]] as VectorValue<*>)
            val dataMatrix = MatrixUtils.createRealMatrix(arrayOf(doubleArray))
            val vector = kltMatrixBar.multiply(dataMatrix.transpose()).getColumnVector(0).toArray()
            //val signature = signatureGenerator.toSignature(vaPlus.getCells(vector, marks))
            val signature = marks.getCells(vector)
            this.signatures.add(VectorApproximationSignature(it.tupleId, signature))
        }
        val meta = VAPlusMeta(marks.marks, signatureGenerator, kltMatrix)
        this.meta.set(meta)
        this.db.commit()
    }


    /**
     * Calculates bound for given signature. What bounds? Square L2 dist?
     *
     * @param cells Signature for the vector from the collection.
     * @param bounds Vector containing bounds for query vector.
     * @param boundsIndex Vector containing indexes for bounds of query vector.
     */
    private fun calculateBounds(cells: IntArray, bounds: FloatArray, boundsIndex: IntArray) = cells.mapIndexed { i, it ->
        val cellsIdx = if (it < 0) {
            ((Short.MAX_VALUE + 1) * 2 + it).toShort().toInt()
        } else {
            it
        }
        bounds[boundsIndex[i] + cellsIdx]
    }.sum()

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