package org.vitrivr.cottontail.dbms.index.pq

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.SquaredEuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.AbstractIndexTest
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.dbms.queries.binding.DefaultBindingContext
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.utilities.math.random.nextDouble
import org.vitrivr.cottontail.utilities.math.random.nextInt
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import java.util.stream.Stream
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * This is a collection of test cases that tests the correct behaviour of [PQIndex] for [FloatVectorValue]s.
 *
 * This class uses artificial data that is pre-clustered, hence, the results of the index lookup become somehow predictable.
 *
 * @author Ralph Gasser
 * @param 1.4.0
 */
@Suppress("UNCHECKED_CAST")
class PQFloatIndexTest : AbstractIndexTest() {

    companion object {
        @JvmStatic
        fun kernels(): Stream<Name.FunctionName> = Stream.of(ManhattanDistance.FUNCTION_NAME, EuclideanDistance.FUNCTION_NAME, SquaredEuclideanDistance.FUNCTION_NAME)
    }

    /** The dimensionality of the test vector. Determined randomly.  */
    private val dimension = this.random.nextInt(128, 2048)

    /** The dimensionality of the test vector. Determined randomly.  */
    private val numberOfClusters = this.random.nextInt(128, 256)

    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Long),
        ColumnDef(this.entityName.column("feature"), Types.FloatVector(this.dimension))
    )

    /** The [ColumnDef] being indexed. */
    override val indexColumn: ColumnDef<FloatVectorValue>
        get() = this.columns[1] as ColumnDef<FloatVectorValue>

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
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val k = (this.numberOfClusters * 0.25).toLong()
        val query = FloatVectorValue(FloatArray(this.indexColumn.type.logicalSize) {
            (this.counter % this.numberOfClusters) + this.random.nextDouble(-1.0, 1.0).toFloat() /* Pre-clustered data. */
        })
        val function = this.catalogue.functions.obtain(Signature.Closed(distance, arrayOf(Argument.Typed(query.type), Argument.Typed(query.type)), Types.Double)) as VectorDistance<*>
        val context = DefaultBindingContext()
        val predicate = ProximityPredicate.NNS(column = this.indexColumn, k = k, distance = function, query = context.bind(query))

        /* Obtain necessary transactions. */
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx
        val index = entityTx.indexForName(this.indexName)
        val indexTx = txn.getTx(index) as IndexTx

        /* Fetch results through full table scan. */
        val bruteForceResults = MinHeapSelection<ComparablePair<TupleId, DoubleValue>>(k.toInt())
        val bruteForceDuration = measureTime {
            val cursor = entityTx.cursor(arrayOf(this.indexColumn))
            cursor.forEach {
                val vector = it[this.indexColumn]
                if (vector is FloatVectorValue) {
                    bruteForceResults.offer(ComparablePair(it.tupleId, function(query, vector)!!))
                }
            }
            cursor.close()
        }

        /* Fetch results through index. */
        val indexResults = ArrayList<Record>(k.toInt())
        val indexDuration = measureTime {
            val cursor = indexTx.filter(predicate)
            cursor.forEach { indexResults.add(it) }
            cursor.close()
        }
        txn.commit()

        /* Calculate an accuracy score for results (since this is an inexact index). */
        var accuracy = 0.0f
        for (i in 0 until k.toInt()) {
            if (indexResults[i].tupleId == bruteForceResults[i].first) accuracy += 1.0f / (k + 1)
        }

        /* Since the data comes pre-clustered, accuracy should always be greater than 90%. */
        Assertions.assertTrue(accuracy > 0.9f)
        Assertions.assertTrue(bruteForceDuration > indexDuration)

        log("Test done for ${function.name} and d=${this.indexColumn.type.logicalSize}! PQ took $indexDuration, brute-force took $bruteForceDuration. Accuracy: $accuracy")
    }

    /**
     * Generates pre-clustered data, which allows control of correctness.
     */
    override fun nextRecord(): StandaloneRecord {
        val id = LongValue(this.counter++)
        val vector = FloatVectorValue(FloatArray(this.indexColumn.type.logicalSize) {
            (this.counter % this.numberOfClusters) + this.random.nextDouble(-1.0, 1.0).toFloat()
        })
        return StandaloneRecord(0L, columns = this.columns, values = arrayOf(id, vector))
    }
}