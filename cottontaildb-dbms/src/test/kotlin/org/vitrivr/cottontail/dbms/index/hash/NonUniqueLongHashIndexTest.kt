package org.vitrivr.cottontail.dbms.index.hash

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.AbstractIndexTest
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import java.util.*

/**
 * This is a collection of test cases to test the correct behaviour of [UQBTreeIndex] with a [LongValue] keys.
 *
 * @author Ralph Gasser
 * @version 1.2.2
 */
class NonUniqueLongHashIndexTest : AbstractIndexTest() {

    /** List of columns for this [NonUniqueStringHashIndexTest]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Long),
        ColumnDef(this.entityName.column("feature"), Types.Double)
    )

    override val indexColumn: ColumnDef<*>
        get() = this.columns.first()

    override val indexName: Name.IndexName
        get() = this.entityName.index("non_unique_long")

    override val indexType: IndexType
        get() = IndexType.BTREE

    /** List of values stored in this [UniqueHashIndexTest]. */
    private var list = HashMap<LongValue, MutableList<DoubleValue>>(100)

    /**
     * Tests if Index#filter() returns the values that have been stored.
     */
    @RepeatedTest(3)
    fun testFilterEqualPositive() {
        /* Obtain necessary transactions. */
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test", this.catalogue, txn)
        try {
            val catalogueTx = this.catalogue.newTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.newTx(ctx)
            val index = entityTx.indexForName(this.indexName)
            val indexTx = index.newTx(ctx)

            /* Prepare binding context and predicate. */
            val columnBinding = ctx.bindings.bind(this.columns[0], this.columns[0])
            val valueBinding = ctx.bindings.bindNull(Types.Long)
            val predicate = BooleanPredicate.Comparison(ComparisonOperator.Equal(columnBinding, valueBinding))

            /* Check all entries. */
            with(ctx.bindings) {
                with(MissingTuple) {
                    for (entry in this@NonUniqueLongHashIndexTest.list.entries) {
                        valueBinding.update(entry.key) /* Update value binding. */
                        var found = false
                        indexTx.filter(predicate).use {
                            while (it.moveNext() && !found) {
                                val rec = entityTx.read(it.key(), this@NonUniqueLongHashIndexTest.columns)
                                val id = rec[this@NonUniqueLongHashIndexTest.columns[0]] as LongValue
                                Assertions.assertEquals(entry.key, id)
                                if (entry.value.contains(rec[this@NonUniqueLongHashIndexTest.columns[1]])) {
                                    found = true
                                }
                            }
                        }
                        Assertions.assertTrue(found)
                    }
                }
            }
        } finally {
            txn.commit()
        }
    }

    /**
     * Tests if Index#filter() only returns stored values.
     */
    @RepeatedTest(3)
    fun testFilterEqualNegative() {
        /* Obtain necessary transactions. */
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("index-test", this.catalogue, txn)
        val catalogueTx = this.catalogue.newTx(ctx)
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = schema.newTx(ctx)
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = entity.newTx(ctx)
        val index = entityTx.indexForName(this.indexName)
        val indexTx = index.newTx(ctx)

        var count = 0
        val predicate = BooleanPredicate.Comparison(ComparisonOperator.Equal(ctx.bindings.bind(this.columns[0], this.columns[0]), ctx.bindings.bind(LongValue(this.random.nextLong(100L, Long.MAX_VALUE)))))
        val cursor = indexTx.filter(predicate)
        cursor.forEach { count += 1 }
        cursor.close()
        Assertions.assertEquals(0, count)
        txn.commit()
    }

    /**
     * Generates and returns a new, random [StandaloneTuple] for inserting into the database.
     */
    override fun nextRecord(): StandaloneTuple {
        val id = LongValue(this.random.nextLong(0L, 100L))
        val value = DoubleValue(this.random.nextDouble())
        if (this.random.nextBoolean() && this.list.size <= 1000) {
            this.list.compute(id) { _, v ->
                val list = v ?: LinkedList()
                list.add(value)
                list
            }
        }
        return StandaloneTuple(0L, columns = this.columns, values = arrayOf(id, value))
    }
}