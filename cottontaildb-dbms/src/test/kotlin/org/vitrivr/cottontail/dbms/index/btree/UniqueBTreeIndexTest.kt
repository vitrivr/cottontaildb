package org.vitrivr.cottontail.dbms.index.btree

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.generators.FloatVectorValueGenerator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.AbstractIndexTest
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import java.util.*

/**
 * This is a collection of test cases to test the correct behaviour of [UQBTreeIndex].
 *
 * @author Ralph Gasser
 * @param 1.2.3
 */
class UniqueBTreeIndexTest : AbstractIndexTest() {

    /** List of columns for this [UniqueBTreeIndexTest]. */
    override val columns = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.String, false) as ColumnDef<*>,
        ColumnDef(this.entityName.column("feature"), Types.FloatVector(128), false) as ColumnDef<*>
    )
    override val indexColumn: ColumnDef<*>
        get() = this.columns.first()

    override val indexName: Name.IndexName
        get() = this.entityName.index("idx_id_unique")

    override val indexType: IndexType
        get() = IndexType.BTREE_UQ

    /** List of values stored in this [UniqueBTreeIndexTest]. */
    private var list = HashMap<StringValue, FloatVectorValue>(100)

    /**
     * Tests if Index#filter() returns the values that have been stored.
     */
    @RepeatedTest(3)
    fun testFilterEqualPositive() {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test", this.instance, txn)

        /* Obtain necessary transactions. */
        val catalogueTx = this.catalogue.createOrResumeTx(ctx)
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = schema.newTx(catalogueTx)
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = entity.createOrResumeTx(schemaTx)
        val index = entityTx.indexForName(this.indexName)
        val indexTx = index.newTx(entityTx)

        /* Prepare binding context and predicate. */
        val columnBinding = ctx.bindings.bind(this.columns[0], this.columns[0])
        val valueBinding = ctx.bindings.bindNull(Types.String)
        val predicate = BooleanPredicate.Comparison(ComparisonOperator.Equal(columnBinding, valueBinding))

        /* Check all entries. */
        with(ctx.bindings) {
            with(MissingTuple) {
                for (entry in this@UniqueBTreeIndexTest.list.entries) {
                    valueBinding.update(entry.key) /* Update value binding. */
                    indexTx.filter(predicate).use { cursor ->
                        cursor.forEach { r ->
                            val rec = entityTx.read(r.tupleId)
                            assertEquals(entry.key.value, (rec[this@UniqueBTreeIndexTest.columns[0]] as StringValue).value)
                            assertArrayEquals(entry.value.data, (rec[this@UniqueBTreeIndexTest.columns[1]] as FloatVectorValue).data)
                        }
                    }
                }
            }
        }

        txn.commit()
    }

    /**
     * Tests if Index#filter() only returns stored values.
     */
    @RepeatedTest(3)
    fun testFilterEqualNegative() {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test", this.instance, txn)

        /* Obtain necessary transactions. */
        val catalogueTx = this.catalogue.createOrResumeTx(ctx)
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = schema.newTx(catalogueTx)
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = entity.createOrResumeTx(schemaTx)
        val index = entityTx.indexForName(this.indexName)
        val indexTx = index.newTx(entityTx)

        var count = 0
        val predicate = BooleanPredicate.Comparison(ComparisonOperator.Equal(ctx.bindings.bind(this.columns[0], this.columns[0]), ctx.bindings.bind(StringValue(UUID.randomUUID().toString()))))
        val cursor = indexTx.filter(predicate)
        cursor.forEach { count += 1 }
        cursor.close()
        assertEquals(0, count)
        txn.commit()
    }

    /**
     * Generates and returns a new, random [StandaloneTuple] for inserting into the database.
     */
    override fun nextRecord(): StandaloneTuple {
        val uuid = StringValue(UUID.randomUUID().toString())
        val vector = FloatVectorValueGenerator.random(128, this.random)
        if (this.random.nextBoolean() && this.list.size <= 1000) {
            this.list[uuid] = vector
        }
        return StandaloneTuple(0L, columns = this.columns, values = arrayOf(uuid, vector))
    }
}
