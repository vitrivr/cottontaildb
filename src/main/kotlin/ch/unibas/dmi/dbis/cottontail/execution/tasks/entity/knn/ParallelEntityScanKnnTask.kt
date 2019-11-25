package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.knn

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnType
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.database.queries.KnnPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.math.knn.ComparablePair
import ch.unibas.dmi.dbis.cottontail.math.knn.HeapSelect
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.Shape
import ch.unibas.dmi.dbis.cottontail.math.knn.metrics.VectorizedDistanceFunction
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Record
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import ch.unibas.dmi.dbis.cottontail.model.values.VectorValue
import com.github.dexecutor.core.task.Task
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

/**
 * A [Task] that executes a parallel boolean kNN on a [Column][ch.unibas.dmi.dbis.cottontail.database.column.Column]
 * of the specified [Entity]. Parallelism is achieved through the use of co-routines.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class ParallelEntityScanKnnTask<T: Any>(val entity: Entity, val knn: KnnPredicate<T>, val predicate: BooleanPredicate? = null, val parallelism: Short = 2) : ExecutionTask("ParallelEntityScanDoubleKnnTask[${entity.fqn}][${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][q=${knn.query.hashCode()}]") {

    /** Set containing the kNN values. */
    private val knnSet = knn.query.map { HeapSelect<ComparablePair<Long,Double>>(this.knn.k) }

    /** List of the [ColumnDef] this instance of [ParallelEntityScanKnnTask] produces. */
    private val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.entity.fqn.append("distance"), ColumnType.forName("DOUBLE")))

    /** The cost of this [ParallelEntityScanKnnTask] is constant */
    override val cost = entity.statistics.columns * (knn.operations + (predicate?.operations ?: 0)).toFloat() / parallelism

    /**
     * Executes this [ParallelEntityScanKnnTask]
     */
    override fun execute(): Recordset {
        /* Extract the necessary data. */
        val columns = arrayOf<ColumnDef<*>>(this.knn.column).plus(predicate?.columns?.toTypedArray() ?: emptyArray())

        /* Execute kNN lookup. */
        this.entity.Tx(readonly = true, columns = columns).begin { tx ->
            val maxTupleId = this.entity.statistics.maxTupleId
            val blocksize = maxTupleId / this.parallelism

            runBlocking {
                val jobs = Array(this@ParallelEntityScanKnnTask.parallelism.toInt()) { j ->
                    GlobalScope.launch {
                        val action: (Record) -> Unit = {
                            val v = it[this@ParallelEntityScanKnnTask.knn.column]
                            if (v is VectorValue<T>) {
                                val distance = this@ParallelEntityScanKnnTask.knn.distance
                                if (distance is VectorizedDistanceFunction) {
                                    this@ParallelEntityScanKnnTask.knn.query.forEachIndexed { i, q ->
                                        if (this@ParallelEntityScanKnnTask.knn.weights != null) {
                                            this@ParallelEntityScanKnnTask.knnSet[i].add(ComparablePair(it.tupleId, distance(q, v, this@ParallelEntityScanKnnTask.knn.weights[i], Shape.S512)))
                                        } else {
                                            this@ParallelEntityScanKnnTask.knnSet[i].add(ComparablePair(it.tupleId, distance(q, v, Shape.S512)))
                                        }
                                    }
                                } else {
                                    this@ParallelEntityScanKnnTask.knn.query.forEachIndexed { i, q ->
                                        if (this@ParallelEntityScanKnnTask.knn.weights != null) {
                                            this@ParallelEntityScanKnnTask.knnSet[i].add(ComparablePair(it.tupleId, distance(q, v, this@ParallelEntityScanKnnTask.knn.weights[i])))
                                        } else {
                                            this@ParallelEntityScanKnnTask.knnSet[i].add(ComparablePair(it.tupleId, distance(q, v)))
                                        }
                                    }
                                }
                            }
                        }

                        if (this@ParallelEntityScanKnnTask.predicate != null) {
                            tx.forEach(blocksize * j + 1L, blocksize * j + blocksize, this@ParallelEntityScanKnnTask.predicate, action)
                        } else {
                            tx.forEach(blocksize * j + 1L, blocksize * j + blocksize, action)
                        }
                    }
                }
                jobs.forEach { it.join() }
            }
            true
        }

        /* Generate dataset and return it. */
        val dataset = Recordset(this.produces)
        for (knn in this.knnSet) {
            for (i in 0 until knn.size) {
                dataset.addRowUnsafe(knn[i].first, arrayOf(DoubleValue(knn[i].second)))
            }
        }
        return dataset
    }
}