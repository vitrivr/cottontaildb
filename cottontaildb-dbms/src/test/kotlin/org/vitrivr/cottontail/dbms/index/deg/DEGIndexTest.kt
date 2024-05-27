package org.vitrivr.cottontail.dbms.index.deg

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
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.AbstractIndexTest
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.index.deg.DEGTest.Companion.K
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.test.TestConstants
import org.vitrivr.cottontail.utilities.formats.FVecsReader
import org.vitrivr.cottontail.utilities.math.ranking.RankingUtilities
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import java.util.stream.Stream
import kotlin.time.Duration
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime


/**
 * A collection of test cases that tests the correct behaviour of [DEGIndex] for [FloatVectorValue]s.
 *
 * This class uses artificial data that is pre-clustered, hence, the results of the index lookup become somehow predictable.
 *
 * @author Ralph Gasser
 * @param 1.0.0
 */
class DEGIndexTest: AbstractIndexTest() {

    companion object {
        @JvmStatic
        fun kernels(): Stream<Name.FunctionName> = Stream.of(
            EuclideanDistance.FUNCTION_NAME
        )
    }

    /** The number of clusters in the test data. */
    private val reader = FVecsReader(this.javaClass.getResourceAsStream("/sift/siftsmall_base.fvecs")!!)

    /** The [collectionSize] used by this [DEGIndexTest]. */
    override val collectionSize: Int
        get() = TestConstants.SIFT_TEST_COLLECTION_SIZE

    /** [ColumnDef] used for retrieval. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Long),
        ColumnDef(this.entityName.column("feature"), Types.FloatVector(128))
    )

    /** The [ColumnDef] being indexed. */
    override val indexColumn: ColumnDef<FloatVectorValue> = this.columns[1] as ColumnDef<FloatVectorValue>

    /** The [Name.IndexName]. */
    override val indexName: Name.IndexName = this.entityName.index("idx_feature_deg")

    /** The [IndexType] being tested. */
    override val indexType: IndexType = IndexType.DEG

    /** The parameters used to create the index. */
    override val indexParams: Map<String, String> = mapOf(
        DEGIndexConfig.KEY_DEGREE to "8",
        DEGIndexConfig.KEY_K_EXT to "16",
        DEGIndexConfig.KEY_EPSILON_EXT to "0.2"
    )

    /** Random number generator. */
    private var counter: Long = 0L

    /**
     * This is a highly artificial test-case for NNS with the [PQIndex] structure for [FloatVectorValue]s.
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
        val k = 10

        /* Fetch results through full table scan. */
        FVecsReader(this.javaClass.getResourceAsStream("/sift/siftsmall_query.fvecs")!!).use { reader ->
            var queries = 0
            var recall = 0.0f
            var bruteForceDuration = Duration.ZERO
            var indexDuration = Duration.ZERO

            /* Prepare data structures. */
            val function = this.catalogue.functions.obtain(
                Signature.Closed(
                    distance,
                    arrayOf(Argument.Typed(Types.FloatVector(128)), Argument.Typed(Types.FloatVector(128))),
                    Types.Double
                )
            ) as VectorDistance<*>
            val idxCol = ctx.bindings.bind(this.indexColumn, this.indexColumn)
            val distCol = ctx.bindings.bind(ColumnDef(Name.ColumnName.create("distance"), Types.Double), null)

            /* Obtain necessary transactions. */
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(catalogueTx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(schemaTx)
            val index = entityTx.indexForName(this.indexName)
            val indexTx = index.newTx(entityTx)

            /* Now do de querying. */
            while (reader.hasNext()) {
                val query = FloatVectorValue(reader.next())
                val predicate = ProximityPredicate.NNS(
                    column = idxCol,
                    distanceColumn = distCol,
                    distance = function,
                    query = ctx.bindings.bind(query),
                    k = k.toLong()
                )

                /* Brute force results.*/
                val bruteForceResults = MinHeapSelection<ComparablePair<TupleId, DoubleValue>>(k)
                bruteForceDuration += measureTime {
                    entityTx.cursor().use { cursor ->
                        cursor.forEach {
                            val vector = it[this.indexColumn]
                            if (vector is FloatVectorValue) {
                                bruteForceResults.offer(ComparablePair(it.tupleId, function(query, vector)!!))
                            }
                        }
                    }
                }

                /* Fetch results through index. */
                val indexResults = ArrayList<ComparablePair<TupleId, DoubleValue>>(K)
                indexDuration += measureTime {
                    indexTx.filter(predicate).use { cursor ->
                        cursor.forEach {
                            indexResults.add(ComparablePair(it.tupleId, it[distCol.column] as DoubleValue))
                        }
                    }
                }
                recall += RankingUtilities.recallAtK(bruteForceResults.toList().map { it.first }, indexResults.map { it.first }, k)
                queries++

                Assertions.assertTrue(k == indexResults.size) { "Number of items retrieved by brute-force search is not equal to k." }
                Assertions.assertTrue(k == bruteForceResults.size) { "Number of items retrieved by indexed search is not equal to k." }
            }
            recall /= queries
            indexDuration /= queries
            bruteForceDuration /= queries

            /* Accuracy for this test should be greater than 80%. */
            Assertions.assertTrue(recall >= 0.8f) { "Recall attained by indexed search is too small (r = $recall)." }
            Assertions.assertTrue(bruteForceDuration >= indexDuration) { "Index search was slower than brute-force (withIndex = $indexDuration, bruteForce = $bruteForceDuration)." }
            log("Search using DEG completed (r = $recall, withIndex = $indexDuration, bruteForce = $bruteForceDuration). Brute-force duration is always in memory!")
        }
    }

    /**
     * Generates pre-clustered data, which allows control of correctness.
     */
    override fun nextRecord(): StandaloneTuple {
        val id = LongValue(this.counter++)
        val vector = FloatVectorValue(this.reader.next())
        return StandaloneTuple(0L, columns = this.columns, values = arrayOf(id, vector))
    }
}