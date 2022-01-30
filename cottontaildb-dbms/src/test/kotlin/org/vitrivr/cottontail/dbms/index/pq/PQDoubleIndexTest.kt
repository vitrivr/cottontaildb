package org.vitrivr.cottontail.dbms.index.pq

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
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionType
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.functions.math.distance.Distances
import org.vitrivr.cottontail.core.queries.functions.math.VectorDistance
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import java.util.*
import java.util.stream.Stream
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * This is a collection of test cases to test the correct behaviour of [PQIndex] for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @param 1.2.2
 */
class PQDoubleIndexTest : AbstractIndexTest() {

    companion object {
        @JvmStatic
        fun kernels(): Stream<Distances> = Stream.of(Distances.L1, Distances.L2, Distances.L2SQUARED, Distances.INNERPRODUCT)
    }

    /** Random number generator. */
    private val random = SplittableRandom()

    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Long),
        ColumnDef(
            this.entityName.column("feature"),
            Types.DoubleVector(this.random.nextInt(128, 2048))
        )
    )

    override val indexColumn: ColumnDef<DoubleVectorValue>
        get() = this.columns[1] as ColumnDef<DoubleVectorValue>

    override val indexName: Name.IndexName
        get() = this.entityName.index("idx_feature_pq")

    override val indexType: IndexType
        get() = IndexType.PQ

    /** Random number generator. */
    private var counter: Long = 0L

    @ParameterizedTest
    @MethodSource("kernels")
    @ExperimentalTime
    fun test(distance: Distances) {
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM)
        val k = 5000
        val query = DoubleVectorValue.random(this.indexColumn.type.logicalSize, this.random)
        val function = this.catalogue.functions.obtain(Signature.Closed(distance.functionName, arrayOf(Argument.Typed(query.type), Argument.Typed(query.type)), Types.Double)) as VectorDistance<*>
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
                if (vector is DoubleVectorValue) {
                    bruteForceResults.offer(ComparablePair(it.tupleId, function(query, vector)))
                }
            }
        }

        /*
        * Calculate an error score for results (since this is an inexact index)
        * TODO: Good metric for testing.
        */
        var found = 0.0f
        for (i in 0 until k) {
            val hit = bruteForceResults[i]
            val idx = indexResults.indexOfFirst { it.tupleId == hit.first }
            if (idx != -1) {
                found += 1.0f
            }
        }
        val foundRatio = (found / k)
        log("Test done for $function and d=${this.indexColumn.type.logicalSize}! PQ took $indexDuration, brute-force took $bruteForceDuration. Found ratio: $foundRatio")
        txn.commit()
    }

    override fun nextRecord(): StandaloneRecord {
        val id = LongValue(this.counter++)
        val vector = DoubleVectorValue.random(this.indexColumn.type.logicalSize, this.random)
        return StandaloneRecord(0L, columns = this.columns, values = arrayOf(id, vector))
    }
}