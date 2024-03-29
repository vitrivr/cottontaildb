package org.vitrivr.cottontail.dbms.index.va

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator
import org.vitrivr.cottontail.dbms.execution.operators.sort.RecordComparator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.AbstractIndexTest
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.utilities.selection.HeapSelection
import java.util.stream.Stream
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * This is a collection of test cases to test the correct behaviour of [VAFIndex] for [FloatVectorValue]s.
 *
 * @author Ralph Gasser
 * @param 1.4.0
 */
@Suppress("UNCHECKED_CAST")
class VAFFloatIndexTest : AbstractIndexTest() {

    companion object {
        @JvmStatic
        fun kernels(): Stream<Name.FunctionName> = Stream.of(ManhattanDistance.FUNCTION_NAME, EuclideanDistance.FUNCTION_NAME)
    }

    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Long),
        ColumnDef(this.entityName.column("feature"), Types.FloatVector(this.random.nextInt(512, 2048)))
    )

    override val indexColumn: ColumnDef<FloatVectorValue>
        get() = this.columns[1] as ColumnDef<FloatVectorValue>

    override val indexName: Name.IndexName
        get() = this.entityName.index("idx_feature_vaf")

    override val indexType: IndexType
        get() = IndexType.VAF

    /** The dimensionality of the test vector. Determined randomly.  */
    private val numberOfClusters = this.random.nextInt(128, 256)

    /** Random number generator. */
    private var counter: Long = 0L

    @ParameterizedTest
    @MethodSource("kernels")
    @ExperimentalTime
    fun test(distance: Name.FunctionName) {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test", this.catalogue, txn)
        try {
            val k = 1000L
            val query = FloatVectorValueGenerator.random(this.indexColumn.type.logicalSize, this.random)
            val function = this.catalogue.functions.obtain(Signature.Closed(distance, arrayOf(Argument.Typed(query.type), Argument.Typed(query.type)), Types.Double)) as VectorDistance<*>
            val idxCol = ctx.bindings.bind(this.indexColumn, this.indexColumn)
            val distCol = ctx.bindings.bind(ColumnDef(Name.ColumnName.create("distance"), Types.Double, nullable = false), null)
            val predicate = ProximityPredicate.NNS(column = idxCol, distanceColumn = distCol, k = k, distance = function, query = ctx.bindings.bind(query))

            /* Obtain necessary transactions. */
            val catalogueTx = this.catalogue.newTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.newTx(ctx)
            val index = entityTx.indexForName(this.indexName)
            val indexTx = index.newTx(ctx)

            /* Fetch results through full table scan. */
            val bruteForceResults = HeapSelection(k.toInt(), RecordComparator.SingleNonNullColumnComparator(predicate.distanceColumn.column, SortOrder.ASCENDING))
            val bruteForceDuration = measureTime {
                entityTx.cursor(arrayOf(this.indexColumn)).use { cursor ->
                    cursor.forEach {
                        bruteForceResults.offer(StandaloneTuple(it.tupleId, arrayOf(idxCol.column, predicate.distanceColumn.column), arrayOf(it[idxCol.column], function(query, it[this.indexColumn]))))
                    }
                }
            }

            /* Fetch results through index. */
            val indexResults = ArrayList<Tuple>(k.toInt())
            val indexDuration = measureTime {
                indexTx.filter(predicate).use { cursor ->
                    cursor.forEach { indexResults.add(it) }
                }
            }

            /* Compare results. */
            for ((e, next) in indexResults.zip(bruteForceResults)) {
                Assertions.assertEquals(next.tupleId, e.tupleId)
                Assertions.assertEquals(next[predicate.distanceColumn.column], e[predicate.distanceColumn.column])
            }

            log("Test done for ${function.name} and d=${this.indexColumn.type.logicalSize}! VAF took $indexDuration, brute-force took $bruteForceDuration.")
        } finally {
            txn.rollback()
        }
    }

    /**
     * Generates pre-clustered data, which allows control of correctness.
     */
    override fun nextRecord(): StandaloneTuple {
        val id = LongValue(this.counter++)
        val vector = FloatVectorValue(FloatArray(this.indexColumn.type.logicalSize) {
            (this.counter % this.numberOfClusters) + this.random.nextDouble(-1.0, 1.0).toFloat()
        })
        return StandaloneTuple(0L, columns = this.columns, values = arrayOf(id, vector))
    }
}
