package org.vitrivr.cottontail.dbms.index.va

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.index.AbstractIndexTest
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.dbms.queries.binding.DefaultBindingContext
import org.vitrivr.cottontail.core.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.core.functions.Argument
import org.vitrivr.cottontail.core.functions.Signature
import org.vitrivr.cottontail.functions.math.distance.Distances
import org.vitrivr.cottontail.core.functions.math.VectorDistance
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import java.util.*
import java.util.stream.Stream
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * This is a collection of test cases to test the correct behaviour of [VAFIndex] for [FloatVectorValue]s.
 *
 * @author Ralph Gasser
 * @param 1.3.1
 */
class VAFFloatIndexTest : AbstractIndexTest() {

    companion object {
        @JvmStatic
        fun kernels(): Stream<Distances> = Stream.of(Distances.L1, Distances.L2, Distances.L2SQUARED)
    }

    /** Random number generator. */
    private val random = SplittableRandom()

    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Long),
        ColumnDef(this.entityName.column("feature"), Types.FloatVector(this.random.nextInt(128, 2048)))
    )

    override val indexColumn: ColumnDef<FloatVectorValue>
        get() = this.columns[1] as ColumnDef<FloatVectorValue>

    override val indexName: Name.IndexName
        get() = this.entityName.index("idx_feature_vaf")

    override val indexType: IndexType
        get() = IndexType.VAF

    /** Random number generator. */
    private var counter: Long = 0L

    @ParameterizedTest
    @MethodSource("kernels")
    @ExperimentalTime
    fun test(distance: Distances) {
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM)
        val k = 100

        val query = FloatVectorValue.random(this.indexColumn.type.logicalSize, this.random)
        val function = this.catalogue.functions.obtain(Signature.Closed(distance.functionName, arrayOf(Argument.Typed(query.type), Argument.Typed(query.type)), Types.Double)) as VectorDistance<*>

        val context = DefaultBindingContext()
        val predicate = KnnPredicate(column = this.indexColumn, k = k, distance = function, query = context.bind(query))

        /* Obtain necessary transactions. */
        val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getTx(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getTx(entity) as EntityTx
        val index = entityTx.indexForName(this.indexName)
        val indexTx = txn.getTx(index) as IndexTx

        /* Fetch results through index. */
        val indexResults = ArrayList<Record>(k)
        val indexDuration = measureTime {
            indexTx.filter(predicate).forEach { indexResults.add(it) }
        }

        /* Fetch results through full table scan. */
        val bruteForceResults = MinHeapSelection<ComparablePair<TupleId, DoubleValue>>(k)
        val bruteForceDuration = measureTime {
            entityTx.scan(arrayOf(this.indexColumn)).forEach {
                val vector = it[this.indexColumn]
                if (vector is FloatVectorValue) {
                    function.provide(1, query)
                    function.provide(0, vector)
                    bruteForceResults.offer(ComparablePair(it.tupleId, function()))
                }
            }
        }

        /* Compare results. */
        for ((i, e) in indexResults.withIndex()) {
            Assertions.assertEquals(bruteForceResults[i].first, e.tupleId)
            Assertions.assertEquals(
                bruteForceResults[i].second,
                e[KnnUtilities.distanceColumnDef(this.entityName)]
            )
        }

        log("Test done for ${distance::class.java.simpleName}! VAF took $indexDuration, brute-force took $bruteForceDuration.")
        txn.commit()
    }

    override fun nextRecord(): StandaloneRecord {
        val id = LongValue(this.counter++)
        val vector = FloatVectorValue.random(this.indexColumn.type.logicalSize, this.random)
        return StandaloneRecord(0L, columns = this.columns, values = arrayOf(id, vector))
    }
}
