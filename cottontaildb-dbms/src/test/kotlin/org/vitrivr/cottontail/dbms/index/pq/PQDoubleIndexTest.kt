package org.vitrivr.cottontail.dbms.index.pq

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.euclidean.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.squaredeuclidean.SquaredEuclideanDistance
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.AbstractIndexTest
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.utilities.math.ranking.RankingUtilities
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import java.util.stream.Stream
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * This is a collection of test cases to test the correct behaviour of [PQIndex] for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @param 1.4.0
 */
@Suppress("UNCHECKED_CAST")
class PQDoubleIndexTest : AbstractIndexTest() {

    companion object {
        @JvmStatic
        fun kernels(): Stream<Name.FunctionName> = Stream.of(ManhattanDistance.FUNCTION_NAME, EuclideanDistance.FUNCTION_NAME, SquaredEuclideanDistance.FUNCTION_NAME)
    }

    /** The dimensionality of the test vector. Determined randomly.  */
    private val dimension = this.random.nextInt(128, 2048)

    /** The dimensionality of the test vector. Determined randomly.  */
    private val numberOfClusters =  512

    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Long),
        ColumnDef(this.entityName.column("feature"), Types.DoubleVector(this.dimension))
    )

    /** The [ColumnDef] being indexed. */
    override val indexColumn: ColumnDef<DoubleVectorValue>
        get() = this.columns[1] as ColumnDef<DoubleVectorValue>

    /** The [Name.IndexName]. */
    override val indexName: Name.IndexName
        get() = this.entityName.index("idx_feature_pq")

    /** The [IndexType] being tested. */
    override val indexType: IndexType
        get() = IndexType.PQ

    /** The parameters used to create the index. */
    override val indexParams: Map<String, String>
        get() = mapOf(PQIndexConfig.KEY_NUM_CENTROIDS to this.numberOfClusters.toString())


    /** Random number generator. */
    private var counter: Long = 0L

    /**
     * This is a highly artificial test-case for NNS with the [PQIndex] structure for [DoubleVectorValue]s.
     *
     * Basically, it revolves around a test-dataset that is arranged in a pre-defined number of clusters, i.e.,
     * the clustering employed by the [PQIndex] can be foreseen. By furthermore keeping k smaller than the number
     * of clusters we make sure, that the number of signatures scanned is always smaller than k, which in general is not
     * a given, since the number of data points is very limited.
     *
     * This test tests the properties of the [PQIndex] in a controlled setting. It should always outperform the brute-force
     * scan in terms of execution time and it should, by design, attain an accuracy of over 90%.
     */
    @ParameterizedTest
    @MethodSource("kernels")
    @ExperimentalTime
    fun test(distance: Name.FunctionName) {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test", this.instance, txn)
        val k = 100
        val query = DoubleVectorValue(DoubleArray(this.indexColumn.type.logicalSize) {
            (this.counter % this.numberOfClusters) + this.random.nextDouble(-1.0, 1.0) /* Pre-clustered data. */
        })
        val function = this.catalogue.functions.obtain(Signature.Closed(distance, arrayOf(Argument.Typed(query.type), Argument.Typed(query.type)), Types.Double)) as VectorDistance<*>
        val idxCol = ctx.bindings.bind(this.indexColumn, this.indexColumn)
        val distCol = ctx.bindings.bind(ColumnDef(Name.ColumnName.create("distance"), Types.Double, nullable = false), null)
        val predicate = ProximityPredicate.Scan(column = idxCol, distanceColumn= distCol, distance = function, query = ctx.bindings.bind(query))

        /* Obtain necessary transactions. */
        val catalogueTx = this.catalogue.createOrResumeTx(ctx)
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = schema.newTx(catalogueTx)
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = entity.createOrResumeTx(schemaTx)
        val index = entityTx.indexForName(this.indexName)
        val indexTx = index.newTx(entityTx)

        /* Fetch results through full table scan. */
        val bruteForceResults = MinHeapSelection<ComparablePair<TupleId, DoubleValue>>(k)
        val bruteForceDuration = measureTime {
            entityTx.cursor().use { cursor ->
                cursor.forEach {
                    val vector = it[this.indexColumn]
                    if (vector is DoubleVectorValue) {
                        bruteForceResults.offer(ComparablePair(it.tupleId, function(query, vector)!!))
                    }
                }
            }
        }

        /* Fetch results through index. */
        val indexResults = MinHeapSelection<ComparablePair<TupleId, DoubleValue>>(k)
        val indexDuration = measureTime {
            indexTx.filter(predicate).use { cursor ->
                cursor.forEach {  indexResults.offer(ComparablePair(it.tupleId, it[distCol.column] as DoubleValue)) }
            }
        }
        txn.commit()

        /* Calculate an accuracy score for results (since this is an inexact index). */
        val recall = RankingUtilities.recallAtK(bruteForceResults.toList().map { it.first }, indexResults.toList().map { it.first }, k)


        /* Since the data comes pre-clustered, accuracy should always be greater than 90%. */
        Assertions.assertTrue(k == bruteForceResults.size) { "Number of items retrieved by brute-force search is not equal to k." }
        Assertions.assertTrue(k == indexResults.size) { "Number of items retrieved by indexed search is not equal to k." }
        Assertions.assertTrue(recall >= 0.9f) { "Recall attained by indexed search is smaller than 90%." }
        Assertions.assertTrue(bruteForceDuration > indexDuration) { "Brute-force search was faster than indexed search." }

        log("Test done for ${function.name} and d=${this.indexColumn.type.logicalSize}! PQ took $indexDuration, brute-force took $bruteForceDuration. Recall: $recall")
    }

    /**
     * Generates pre-clustered data, which allows control of correctness.
     */
    override fun nextRecord(): StandaloneTuple {
        val id = LongValue(this.counter++)
        val vector = DoubleVectorValue(DoubleArray(this.indexColumn.type.logicalSize) {
            (this.counter % this.numberOfClusters) + this.random.nextDouble(-1.0, 1.0)
        })
        return StandaloneTuple(0L, columns = this.columns, values = arrayOf(id, vector))
    }
}