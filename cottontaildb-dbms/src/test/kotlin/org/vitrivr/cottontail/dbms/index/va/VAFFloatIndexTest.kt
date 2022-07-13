package org.vitrivr.cottontail.dbms.index.va

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.sort.RecordComparator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.AbstractIndexTest
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.binding.DefaultBindingContext
import org.vitrivr.cottontail.dbms.schema.SchemaTx
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
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val k = 1000L
            val query = FloatVectorValueGenerator.random(this.indexColumn.type.logicalSize, this.random)
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

            val bruteForceResults = HeapSelection(k, RecordComparator.SingleNonNullColumnComparator(predicate.distanceColumn, SortOrder.ASCENDING))
            val bruteForceDuration = measureTime {
                entityTx.cursor(arrayOf(this.indexColumn)).use { cursor ->
                    cursor.forEach {
                        bruteForceResults.offer(StandaloneRecord(it.tupleId, arrayOf(this.indexColumn, predicate.distanceColumn), arrayOf(it[this.indexColumn], function(query, it[this.indexColumn]))))
                    }
                }
            }

            /* Fetch results through index. */
            val indexResults = ArrayList<Record>(k.toInt())
            val indexDuration = measureTime {
                indexTx.filter(predicate).use { cursor ->
                    cursor.forEach { indexResults.add(it) }
                    cursor.close()
                }
            }

            /* Compare results. */
            for ((i, e) in indexResults.withIndex()) {
                Assertions.assertEquals(bruteForceResults[i.toLong()].tupleId, e.tupleId)
                Assertions.assertEquals(bruteForceResults[i.toLong()][predicate.distanceColumn], e[predicate.distanceColumn])
            }

            log("Test done for ${function.name} and d=${this.indexColumn.type.logicalSize}! VAF took $indexDuration, brute-force took $bruteForceDuration.")
        } finally {
            txn.rollback()
        }
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
