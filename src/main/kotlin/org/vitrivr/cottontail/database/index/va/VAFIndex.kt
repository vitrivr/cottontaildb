package org.vitrivr.cottontail.database.index.va

import org.mapdb.Atomic
import org.mapdb.DBMaker
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.lsh.superbit.SuperBitLSH
import org.vitrivr.cottontail.database.index.lsh.superbit.SuperBitLSHIndex
import org.vitrivr.cottontail.database.index.pq.PQIndex
import org.vitrivr.cottontail.database.index.va.marks.Marks
import org.vitrivr.cottontail.database.index.va.marks.MarksGenerator
import org.vitrivr.cottontail.database.queries.components.AtomicBooleanPredicate
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.math.knn.metrics.AbsoluteInnerProductDistance
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.MinSingleSelection
import org.vitrivr.cottontail.math.knn.selection.Selection
import org.vitrivr.cottontail.model.basics.CloseableIterator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.types.ComplexVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.extensions.write

import java.nio.file.Path
import java.util.concurrent.Callable
import java.util.concurrent.Executors

import kotlin.math.max
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.sign

class VAFIndex(override val name: Name.IndexName, override val parent: Entity, override val columns: Array<ColumnDef<*>>): Index() {

    companion object {
        val SIGNATURE_FIELD_NAME = "vaf_signatures"
        val REAL_MARKS_FIELD_NAME = "vaf_marks_real"
        val IMAG_MARKS_FIELD_NAME = "vaf_marks_imag"
        val MARKS_PER_DIM = 50  // doesn't have too much of an influence on execution time with current implementation -> something is quite inefficient... Influence on filter ratio is visible
        val LOGGER: Logger = LoggerFactory.getLogger(VAFIndex::class.java)
    }
    /** The [Path] to the [DBO]'s main file OR folder. */
    override val path: Path = this.parent.path.resolve("idx_vaf_$name.db")

    /** The [VAFIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.parent.name.column("distance"), ColumnType.forName("DOUBLE")))

    /** The type of [Index]. */
    override val type = IndexType.VAF

    /** The internal [DB] reference. */
    private val db = if (parent.parent.parent.config.mapdb.forceUnmap) {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().cleanerHackEnable().transactionEnable().make()
    } else {
        DBMaker.fileDB(this.path.toFile()).fileMmapEnable().transactionEnable().make()
    }

    /** Store for the [Marks] (real). */
    private val marksRealStore: Atomic.Var<Marks> = this.db.atomicVar(REAL_MARKS_FIELD_NAME, Marks.MarksSerializer).createOrOpen()

    /** Store for the [Marks] (imaginary). */
    private val marksImagStore: Atomic.Var<Marks> = this.db.atomicVar(IMAG_MARKS_FIELD_NAME, Marks.MarksSerializer).createOrOpen()

    /** Store for the signatures. */
    private val signatures = this.db.indexTreeList(SIGNATURE_FIELD_NAME, VectorApproximationSignatureSerializer).createOrOpen()

    /** List of real [VectorApproximationSignature]s. */
    private var cachedSignaturesReal = mutableListOf<VectorApproximationSignature>()

    /** List of real [VectorApproximationSignature]s. */
    private var cachedSignaturesImaginary = mutableListOf<VectorApproximationSignature>()

    init {
        if (columns.size != 1) {
            throw DatabaseException.IndexNotSupportedException(name, "${this::class.java} currently only supports indexing a single column")
        }
        if (!columns.all { it.type == ColumnType.forName("COMPLEX32_VEC") || it.type == ColumnType.forName("COMPLEX64_VEC") }) {
            throw DatabaseException.IndexNotSupportedException(name, "${this::class.java} currently only supports indexing complex vector columns, not ${columns.first()::class.java}")
        }
        updateSignatureCacheFromStore()
    }

    /**
     * Flag indicating if this [VAFIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Closes this [VAFIndex] and the associated data structures.
     */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }

    /**
     * Calculates the cost estimate if this [Index] processing the provided [Predicate].
     * todo: get real cost estimate
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate) = Cost.ZERO

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [VAFIndex].
     *
     * Note: Only use the innerproduct distances with normalized vectors!
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) =
            predicate is KnnPredicate<*>
                    && predicate.query.all { it is ComplexVectorValue<*> }
                    && predicate.columns.first() == this.columns[0]
                    && predicate.distance is AbsoluteInnerProductDistance



    /**
     * Reloads all [VectorApproximationSignature]s from the underlying data store.
     */
    private fun updateSignatureCacheFromStore() {
        this.cachedSignaturesReal.clear()
        this.cachedSignaturesImaginary.clear()
        if (this.columns[0].type.complex) {
            this.signatures.forEachIndexed { index, vectorApproximationSignature ->
                if (index % 2 == 0) {
                    this.cachedSignaturesReal.add(vectorApproximationSignature!!)
                } else {
                    this.cachedSignaturesImaginary.add(vectorApproximationSignature!!)
                }
            }
        } else {
            this.signatures.forEachIndexed { index, vectorApproximationSignature ->
                this.cachedSignaturesReal.add(vectorApproximationSignature!!)
            }
        }
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [PQIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Tries to find a specimen of the [VectorValue] in the [Entity] underpinning this [SuperBitLSHIndex]
     *
     * @param tx [Entity.Tx] used to read from [Entity]
     * @return A specimen of the [VectorValue] that should be indexed.
     */
    private fun acquireSpecimen(tx: EntityTx): VectorValue<*>? {
        for (index in 0L until tx.maxTupleId()) {
            val read = tx.read(index, this@VAFIndex.columns)[this.columns[0]]
            if (read is VectorValue<*>) {
                return read
            }
        }
        return null
    }

    /**
     * A [IndexTx] that affects this [Index].
     */
    private inner class Tx(context: TransactionContext) : Index.Tx(context) {

        /**
         * (Re-)builds the [VAFIndex] from scratch.
         */
        override fun rebuild() = this.withWriteLock {
            LOGGER.trace("Rebuilding VAF index {}", this@VAFIndex.name)

            /* Obtain a transaction on the entity. */
            val txn = this.context.getTx(this.dbo.parent) as EntityTx

            /* Rebuilds the index. Algorithm differs for comples and real values. */
            if (this@VAFIndex.columns[0].type.complex) {
                this.rebuildComplex(txn)
            } else {
                this.rebuildReal(txn)
            }

            LOGGER.trace("Done rebuilding VAF index {}", this@VAFIndex.name)
        }

        /**
         * Updates the [VAFIndex] with the provided [DataChangeEvent]s. This method determines, whether the [Record]
         * affected by the [DataChangeEvent] should be added or updated
         *
         * @param update Collection of [DataChangeEvent]s to process.
         */
        override fun update(update: Collection<DataChangeEvent>) = this.withWriteLock {
            TODO("Not yet implemented")
        }

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [CloseableIterator] ofall [TupleId]s that match
         * the [Predicate]. Only supports [KnnPredicate]s.
         *
         * The [CloseableIterator] is not thread safe!
         *
         * <strong>Important:</strong> It remains to the caller to close the [CloseableIterator]
         *
         * @param predicate The [Predicate] for the lookup*
         * @return The resulting [CloseableIterator]
         */
        override fun filter(predicate: Predicate): CloseableIterator<Record> = object : CloseableIterator<Record> {

            /** Cast [AtomicBooleanPredicate] (if such a cast is possible).  */
            val predicate = if (predicate is KnnPredicate<*>) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@VAFIndex.name}' (VAF Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            override fun hasNext(): Boolean {
                TODO()
            }

            override fun next(): Record {
               TODO()
            }

            override fun close() {
                if (!this.closed) {
                    this.closed = true
                }
            }

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            private var closed = false

            /** The index of the current query vector. */
            private var queryIndex = -1
        }

        /**
         * Rebuilds the index for real-valued vectors.
         *
         * @param txn The [EntityTx] to rebuild the [VAFIndex] with.
         */
        private fun rebuildReal(txn: EntityTx) {
            /* Find minimum and maximum for dimensions to obtain marks. */
            val min = DoubleArray(this@VAFIndex.columns[0].logicalSize)
            val max = DoubleArray(this@VAFIndex.columns[0].logicalSize)
            LOGGER.debug("Finding min and max for each dim")
            txn.scan(this@VAFIndex.columns).forEach { r ->
                val value = r[this@VAFIndex.columns[0]] as VectorValue<*>
                for (i in 0 until value.logicalSize) {
                    min[i] = min(min[i], value[i].asDouble().value)
                    max[i] = max(max[i], value[i].asDouble().value)
                }
            }

            /* Calculate and update marks. */
            val marks = MarksGenerator.getEquidistantMarks(min, max, IntArray(this@VAFIndex.columns[0].logicalSize) { MARKS_PER_DIM })
            this@VAFIndex.marksRealStore.set(marks)

            /* Calculate and update signatures. */
            LOGGER.debug("Generating signatures for vectors")
            txn.scan(this@VAFIndex.columns).forEach { r ->
                val value = r[this@VAFIndex.columns[0]] as VectorValue<*>
                val converted = (0 until value.logicalSize).map { value[it].value.toDouble() }.toDoubleArray()
                this@VAFIndex.signatures.add(VectorApproximationSignature(r.tupleId, marks.getCells(converted)))
            }
        }

        /**
         * Rebuilds the index for complex-valued vectors.
         *
         * @param txn The [EntityTx] to rebuild the [VAFIndex] with.
         */
        private fun rebuildComplex(txn: EntityTx) {
            val minReal = DoubleArray(this@VAFIndex.columns[0].logicalSize)
            val minImag = DoubleArray(this@VAFIndex.columns[0].logicalSize)
            val maxReal = DoubleArray(this@VAFIndex.columns[0].logicalSize)
            val maxImag = DoubleArray(this@VAFIndex.columns[0].logicalSize)
            LOGGER.debug("Finding min and max for each dim")
            txn.scan(this@VAFIndex.columns).forEach { r ->
                (r[columns.first()] as ComplexVectorValue<*>).forEachIndexed { i, v ->
                    minReal[i] = min(minReal[i], v.real.value.toDouble())
                    minImag[i] = min(minImag[i], v.imaginary.value.toDouble())
                    maxReal[i] = max(maxReal[i], v.real.value.toDouble())
                    maxImag[i] = max(maxImag[i], v.imaginary.value.toDouble())
                }
            }
            val marksReal = MarksGenerator.getEquidistantMarks(minReal, maxReal, IntArray(this@VAFIndex.columns[0].logicalSize) { MARKS_PER_DIM })
            this@VAFIndex.marksRealStore.set(marksReal)
            val marksImag = MarksGenerator.getEquidistantMarks(minImag, maxImag, IntArray(this@VAFIndex.columns[0].logicalSize) { MARKS_PER_DIM })
            this@VAFIndex.marksImagStore.set(marksImag)

            LOGGER.debug("Generating signatures for vectors")
            txn.scan(this@VAFIndex.columns).forEach { r ->
                val (valueReal, valueImag) = (r[columns[0]] as ComplexVectorValue<*>).map { it.real.value.toDouble() to it.imaginary.value.toDouble() }.unzip()
                this@VAFIndex.signatures.add(VectorApproximationSignature(r.tupleId, marksReal.getCells(valueReal.toDoubleArray())))
                this@VAFIndex.signatures.add(VectorApproximationSignature(r.tupleId, marksImag.getCells(valueImag.toDoubleArray())))
            }
            LOGGER.info("Done.")
        }

        /**
         * Commits changes and update signature cache.
         */
        override fun performCommit() {
            this@VAFIndex.db.commit()
            this@VAFIndex.updateSignatureCacheFromStore()
        }

        /**
         * Makes a rollback on all changes to the index.
         */
        override fun performRollback() {
            this@VAFIndex.db.commit()
        }
    }

    /**
     * Performs a lookup through this [Index] and returns [Recordset]. This is an internal method! External
     * invocation is only possible through a [Index.Tx] object.
     *
     * This is the minimal method any [Index] implementation must support.
     *
     * @param predicate The [Predicate] to perform the lookup.
     * @param tx Reference to the [Entity.Tx] the call to this method belongs to.
     * @return The resulting [Recordset].
     *
     * @throws QueryException.UnsupportedPredicateException If predicate is not supported by [Index].
     */
    override fun filter(predicate: Predicate, tx: Entity.Tx): Recordset {
        require(canProcess(predicate)) { "The supplied predicate $predicate is not supported by the index" }
        LOGGER.info("filtering")
        predicate as KnnPredicate<*>
        // need as array of primitives for performance
        val queriesSplit = Array(predicate.query.size) {i ->
            DoubleArray(predicate.query[i].logicalSize) {j ->
                (predicate.query[i] as ComplexVectorValue<*>).real(j).value.toDouble()
            } to DoubleArray(predicate.query[i].logicalSize) { j ->
                (predicate.query[i] as ComplexVectorValue<*>).imaginary(j).value.toDouble()
            }
        }
        LOGGER.debug("Precomputing all products of queries and marks")
        /* each query gets a 4-array with product of query and marks for real-real, imag-imag, real-imag, imag-real
           (query componentpart - marks)
         */
        val queriesMarksProducts = Array(queriesSplit.size) {i ->
            arrayOf(
                QueryMarkProducts(queriesSplit[i].first, marksReal),
                QueryMarkProducts(queriesSplit[i].second, marksImag),
                QueryMarkProducts(queriesSplit[i].first, marksImag),
                QueryMarkProducts(queriesSplit[i].second, marksReal)
            )
        }
        LOGGER.debug("Done")
        var countCandidates = 0
        var countRejected = 0
        LOGGER.debug("scanning records")
//        val triple = filterPartSimpleNoParallel(0, signaturesReal.lastIndex, predicate, queriesMarksProducts, tx)
        val triple = filterParallel(predicate, queriesMarksProducts, tx)
        val knns = triple.first
        countCandidates = triple.second
        countRejected = triple.third
        LOGGER.info("Done. Considered candidates: $countCandidates, rejected candidates: $countRejected (${countRejected.toDouble() / (countRejected + countCandidates) * 100} %)")

        return selectToRecordset(this.produces.first(), knns.toList())
    }




    private fun filterPartSimpleNoParallel(start: Int, endInclusive: Int, predicate: KnnPredicate<*>, queriesMarksProducts: Array<Array<QueryMarkProducts>>, tx: Entity.Tx): Triple<List<Selection<ComparablePair<Long,DoubleValue>>>, Int, Int> {
        LOGGER.debug("filtering from $start to $endInclusive")
        val knns = predicate.query.map {
            if (predicate.k == 1) MinSingleSelection<ComparablePair<Long, DoubleValue>>() else MinHeapSelection<ComparablePair<Long, DoubleValue>>(predicate.k)
        }
        var countCandidates1 = 0
        var countRejected1 = 0
        (start .. endInclusive).forEach {
            val sigReal = cachedSignaturesReal[it]
            val sigImag = cachedSignaturesImaginary[it]
            predicate.query.forEachIndexed { i, query ->
                val absIPDistLB = 1.0 - absoluteComplexInnerProductSqUpperBoundCached2(sigReal.signature, sigImag.signature, queriesMarksProducts[i][0], queriesMarksProducts[i][1], queriesMarksProducts[i][2], queriesMarksProducts[i][3]).pow(0.5)
                if (knns[i].size < predicate.k || knns[i].peek()!!.second > absIPDistLB) {
                    countCandidates1++
                    val tid = sigReal.tupleId
                    knns[i].offer(ComparablePair(tid, predicate.distance((tx.read(tid)[columns.first()] as ComplexVectorValue<*>), query)))
                } else {
                    countRejected1++
                }
            }
        }
        LOGGER.debug("done filtering from $start to $endInclusive")
        return Triple(knns, countCandidates1, countRejected1)
    }

    private fun filterParallel(predicate: KnnPredicate<*>, queriesMarksProducts: Array<Array<QueryMarkProducts>>, tx: Entity.Tx): Triple<List<Selection<ComparablePair<Long,DoubleValue>>>, Int, Int> {
        //split signatures to threads
        val numThreads = 2
        val elemsPerThread = cachedSignaturesReal.size / numThreads
        val remaining = cachedSignaturesReal.size % numThreads
        val exec = Executors.newFixedThreadPool(numThreads)
        val tasks = (0 until numThreads).map {
            Callable { filterPartSimpleNoParallel(it * elemsPerThread,
                    it * elemsPerThread + elemsPerThread - 1 + if (it == numThreads - 1) remaining else 0,
            predicate, queriesMarksProducts, tx)}
        }
        val fresults = exec.invokeAll(tasks)
        val res = fresults.map { it.get()}
        exec.shutdownNow()
        // merge
        LOGGER.debug("Merging results")
        return res.reduce { acc, perThread ->
            Triple((perThread.first zip acc.first).map { (knnPerThread, knnAcc) ->
                knnAcc.apply {
                    for (i in 0 until knnPerThread.size) offer(knnPerThread[i])
                }
            }, perThread.second + acc.second, perThread.third + acc.third)
        }
    }
}

/*
Blott & Weber 1997 p.8 top
 */
fun upperBoundComponentDifferences(cellsVec: IntArray, query: DoubleArray, marks: Marks): List<Double> {
    val cellsQuery = marks.getCells(query)
    return (cellsVec zip cellsQuery).mapIndexed { i, (v, cq) ->
        val a = { query[i] - marks.marks[i][v] }
        val b = { marks.marks[i][v + 1] - query[i] }
        when {
            v < cq -> {
                a()
            }
            v == cq -> {
                max(a(), b())
            }
            else -> {
                b()
            }
        }
    }
}

/*
Blott & Weber 1997 p.8 top
 */
fun lowerBoundComponentDifferences(cellsVec: IntArray, query: DoubleArray, marks: Marks): List<Double> {
    val cellsQuery = marks.getCells(query)
    return (cellsVec zip cellsQuery).mapIndexed { i, (v, q) ->
        when {
            v < q -> {
                query[i] - marks.marks[i][v + 1]
            }
            v == q -> {
                0.0
            }
            else -> {
                marks.marks[i][v] - query[i]
            }
        }
    }
}

/*
Takes cells (approximation from a DB vector) and another Vector (query) and builds the lower-bound of the
sum of element-by-element products (i.e. it returns a lower bound on the real dot product)
 */
fun lowerBoundComponentProductsSum(cellsVec: IntArray, query: DoubleArray, marks: Marks): Double {
    return cellsVec.mapIndexed { i, cv ->
        if (query[i] < 0) {
            marks.marks[i][cv + 1] * query[i]
        } else {
            marks.marks[i][cv] * query[i]
        }
    }.sum()
}

fun lowerBoundComponentProductsSum(cellsVec: IntArray, componentProducts: QueryMarkProducts): Double {
    return cellsVec.mapIndexed { i, cv ->
        min(componentProducts.value[i][cv], componentProducts.value[i][cv + 1])
    }.sum()
}

/*
Takes cells (approximation from a real DB vector) and another real Vector (query) and builds the upper-bound of the
sum of element-by-element products (i.e. it returns an upper bound on the real dot product).
Real vector means that this can be a vector of real parts or a vector of imaginary parts.
 */
fun upperBoundComponentProductsSum(cellsVec: IntArray, query: DoubleArray, marks: Marks): Double {
    return cellsVec.mapIndexed { i, cv ->
        if (query[i] < 0) {
            marks.marks[i][cv] * query[i]
        } else {
            marks.marks[i][cv + 1] * query[i]
        }
    }.sum()
}

fun upperBoundComponentProductsSum(cellsVec: IntArray, componentProducts: QueryMarkProducts): Double {
    return cellsVec.mapIndexed { i, cv ->
        max(componentProducts.value[i][cv], componentProducts.value[i][cv + 1])
    }.sum()
}

/**
 * is private to remove notnull checks in func...
 */
private inline fun lowerUpperBoundComponentProductsSum(cellsVec: IntArray, componentProducts: QueryMarkProducts, outArray: DoubleArray) {
    var a = 0.0
    var b = 0.0
    cellsVec.forEachIndexed { i, cv ->
        val c = componentProducts.value[i][cv]
        val d = componentProducts.value[i][cv + 1]
        if (c < d) {
            a += c
            b += d
        } else {
            a += d
            b += c
        }
    }
    outArray[0] = a
    outArray[1] = b
}

fun realDotProductBounds(cellsVec: IntArray, query: DoubleArray, marks: Marks): Pair<Double, Double> =
        lowerBoundComponentProductsSum(cellsVec, query, marks) to upperBoundComponentProductsSum(cellsVec, query, marks)

fun ubComplexInnerProductImag(cellsVecImag: IntArray, queryReal: DoubleArray, cellsVecReal: IntArray, queryImag: DoubleArray, marksReal: Marks, marksImag: Marks) =
        upperBoundComponentProductsSum(cellsVecImag, queryReal, marksImag) - lowerBoundComponentProductsSum(cellsVecReal, queryImag, marksReal)

fun ubComplexInnerProductReal(cellsVecReal: IntArray, queryReal: DoubleArray, cellsVecImag: IntArray, queryImag: DoubleArray, marksReal: Marks, marksImag: Marks) =
        upperBoundComponentProductsSum(cellsVecReal, queryReal, marksReal) + upperBoundComponentProductsSum(cellsVecImag, queryImag, marksImag)

fun lbComplexInnerProductImag(cellsVecImag: IntArray, queryReal: DoubleArray, cellsVecReal: IntArray, queryImag: DoubleArray, marksReal: Marks, marksImag: Marks) =
        lowerBoundComponentProductsSum(cellsVecImag, queryReal, marksImag) - upperBoundComponentProductsSum(cellsVecReal, queryImag, marksReal)

fun lbComplexInnerProductReal(cellsVecReal: IntArray, queryReal: DoubleArray, cellsVecImag: IntArray, queryImag: DoubleArray, marksReal: Marks, marksImag: Marks) =
        lowerBoundComponentProductsSum(cellsVecReal, queryReal, marksReal) + lowerBoundComponentProductsSum(cellsVecImag, queryImag, marksImag)

fun ubAbsoluteComplexInnerProductSq(lbIPReal: Double, ubIPReal: Double, lbIPImag: Double, ubIPImag: Double) =
        max(lbIPReal.pow(2), ubIPReal.pow(2)) + max(lbIPImag.pow(2), ubIPImag.pow(2))


fun lbAbsoluteComplexInnerProductSq(lbIPReal: Double, ubIPReal: Double, lbIPImag: Double, ubIPImag: Double) =
        (if (lbIPReal.sign != ubIPReal.sign) {
            0.0
        } else {
            min(lbIPReal.pow(2), ubIPReal.pow(2))
        }
                +
                if (lbIPImag.sign != ubIPImag.sign) {
                    0.0
                } else {
                    min(lbIPImag.pow(2), ubIPImag.pow(2))
                })

fun absoluteComplexInnerProductSqBounds(cellsVecReal: IntArray, cellsVecImag: IntArray, queryReal: DoubleArray, queryImag: DoubleArray, marksReal: Marks, marksImag: Marks): Pair<Double, Double> {
    val lbDPReal = lbComplexInnerProductReal(cellsVecReal, queryReal, cellsVecImag, queryImag, marksReal, marksImag)
    val lbDPImag = lbComplexInnerProductImag(cellsVecImag, queryReal, cellsVecReal, queryImag, marksReal, marksImag)
    val ubDPReal = ubComplexInnerProductReal(cellsVecReal, queryReal, cellsVecImag, queryImag, marksReal, marksImag)
    val ubDPImag = ubComplexInnerProductImag(cellsVecImag, queryReal, cellsVecReal, queryImag, marksReal, marksImag)
    return lbAbsoluteComplexInnerProductSq(lbDPReal, ubDPReal, lbDPImag, ubDPImag) to ubAbsoluteComplexInnerProductSq(lbDPReal, ubDPReal, lbDPImag, ubDPImag)
}

fun absoluteComplexInnerProductSqUpperBound(cellsVecReal: IntArray, cellsVecImag: IntArray, queryReal: DoubleArray, queryImag: DoubleArray, marksReal: Marks, marksImag: Marks): Double {
    val lbDPReal = lbComplexInnerProductReal(cellsVecReal, queryReal, cellsVecImag, queryImag, marksReal, marksImag)
    val lbDPImag = lbComplexInnerProductImag(cellsVecImag, queryReal, cellsVecReal, queryImag, marksReal, marksImag)
    val ubDPReal = ubComplexInnerProductReal(cellsVecReal, queryReal, cellsVecImag, queryImag, marksReal, marksImag)
    val ubDPImag = ubComplexInnerProductImag(cellsVecImag, queryReal, cellsVecReal, queryImag, marksReal, marksImag)
    return ubAbsoluteComplexInnerProductSq(lbDPReal, ubDPReal, lbDPImag, ubDPImag)
}
fun absoluteComplexInnerProductSqUpperBoundInlined(cellsVecReal: IntArray, cellsVecImag: IntArray, queryReal: DoubleArray, queryImag: DoubleArray, marksReal: Marks, marksImag: Marks): Double {
    val lbIPReal = lowerBoundComponentProductsSum(cellsVecReal, queryReal, marksReal) + lowerBoundComponentProductsSum(cellsVecImag, queryImag, marksImag)
    val lbIPImag = lowerBoundComponentProductsSum(cellsVecImag, queryReal, marksImag) - upperBoundComponentProductsSum(cellsVecReal, queryImag, marksReal)
    val ubIPReal = upperBoundComponentProductsSum(cellsVecReal, queryReal, marksReal) + upperBoundComponentProductsSum(cellsVecImag, queryImag, marksImag)
    val ubIPImag = upperBoundComponentProductsSum(cellsVecImag, queryReal, marksImag) - lowerBoundComponentProductsSum(cellsVecReal, queryImag, marksReal)
    return max(lbIPReal.pow(2), ubIPReal.pow(2)) + max(lbIPImag.pow(2), ubIPImag.pow(2))
}

inline fun absoluteComplexInnerProductSqUpperBoundCached(cellsVecReal: IntArray,
                                                  cellsVecImag: IntArray,
                                                  queryMarkProductsRealReal: QueryMarkProducts,
                                                  queryMarkProductsImagImag: QueryMarkProducts,
                                                  queryMarkProductsRealImag: QueryMarkProducts,
                                                  queryMarkProductsImagReal: QueryMarkProducts): Double {
    val lbIPReal = lowerBoundComponentProductsSum(cellsVecReal, queryMarkProductsRealReal) + lowerBoundComponentProductsSum(cellsVecImag, queryMarkProductsImagImag)
    val lbIPImag = lowerBoundComponentProductsSum(cellsVecImag, queryMarkProductsRealImag) - upperBoundComponentProductsSum(cellsVecReal, queryMarkProductsImagReal)
    val ubIPReal = upperBoundComponentProductsSum(cellsVecReal, queryMarkProductsRealReal) + upperBoundComponentProductsSum(cellsVecImag, queryMarkProductsImagImag)
    val ubIPImag = upperBoundComponentProductsSum(cellsVecImag, queryMarkProductsRealImag) - lowerBoundComponentProductsSum(cellsVecReal, queryMarkProductsImagReal)
    return max(lbIPReal.pow(2), ubIPReal.pow(2)) + max(lbIPImag.pow(2), ubIPImag.pow(2))
}

private inline fun absoluteComplexInnerProductSqUpperBoundCached2(cellsVecReal: IntArray,
                                                   cellsVecImag: IntArray,
                                                   queryMarkProductsRealReal: QueryMarkProducts,
                                                   queryMarkProductsImagImag: QueryMarkProducts,
                                                   queryMarkProductsRealImag: QueryMarkProducts,
                                                   queryMarkProductsImagReal: QueryMarkProducts): Double {
    val realRealRealBounds = DoubleArray(2)
    lowerUpperBoundComponentProductsSum(cellsVecReal, queryMarkProductsRealReal, realRealRealBounds)
    val imagImagImagBounds = DoubleArray(2)
    lowerUpperBoundComponentProductsSum(cellsVecImag, queryMarkProductsImagImag, imagImagImagBounds)
    val imagRealImagBounds = DoubleArray(2)
    lowerUpperBoundComponentProductsSum(cellsVecImag, queryMarkProductsRealImag, imagRealImagBounds)
    val realImagRealBounds = DoubleArray(2)
    lowerUpperBoundComponentProductsSum(cellsVecReal, queryMarkProductsImagReal, realImagRealBounds)
    return max((realRealRealBounds[0] + imagImagImagBounds[0]).pow(2), (realRealRealBounds[1] + imagImagImagBounds[1]).pow(2)) + max((imagRealImagBounds[0] - realImagRealBounds[1]).pow(2), (imagRealImagBounds[1] - realImagRealBounds[0]).pow(2))
}

fun absoluteComplexInnerProductSqUpperBoundCached2Public(cellsVecReal: IntArray,
                                                           cellsVecImag: IntArray,
                                                           queryMarkProductsRealReal: QueryMarkProducts,
                                                           queryMarkProductsImagImag: QueryMarkProducts,
                                                           queryMarkProductsRealImag: QueryMarkProducts,
                                                           queryMarkProductsImagReal: QueryMarkProducts): Double {
    return absoluteComplexInnerProductSqUpperBoundCached2(cellsVecReal,
            cellsVecImag,
            queryMarkProductsRealReal,
            queryMarkProductsImagImag,
            queryMarkProductsRealImag,
            queryMarkProductsImagReal)
}