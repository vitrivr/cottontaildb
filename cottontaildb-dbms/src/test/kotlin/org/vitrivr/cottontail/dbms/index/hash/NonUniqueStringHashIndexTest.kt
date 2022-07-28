package org.vitrivr.cottontail.dbms.index.hash

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.RepeatedTest
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.generators.LongValueGenerator
import org.vitrivr.cottontail.core.values.generators.StringValueGenerator
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.AbstractIndexTest
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.binding.DefaultBindingContext
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import java.util.*

/**
 * This is a collection of test cases to test the correct behaviour of [UQBTreeIndex] with a [StringValue].
 *
 * @author Ralph Gasser
 * @version 1.2.3
 */
class NonUniqueStringHashIndexTest : AbstractIndexTest() {

    /** List of columns for this [NonUniqueStringHashIndexTest]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.String, false),
        ColumnDef(this.entityName.column("feature"), Types.Long, false)
    )

    override val indexColumn: ColumnDef<*>
        get() = this.columns.first()

    override val indexName: Name.IndexName
        get() = this.entityName.index("non_unique_string")

    override val indexType: IndexType
        get() = IndexType.BTREE

    /** List of values stored in this [UniqueHashIndexTest]. */
    private var list = HashMap<StringValue, MutableList<LongValue>>(100)

    /**
     * Tests if Index#filter() returns the values that have been stored.
     */
    @RepeatedTest(3)
    fun testFilterEqualPositive() {
        /* Obtain necessary transactions. */
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val catalogueTx = txn.getCachedTxForDBO(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getCachedTxForDBO(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = txn.getCachedTxForDBO(entity) as EntityTx
            val index = entityTx.indexForName(this.indexName)
            val indexTx = txn.getCachedTxForDBO(index) as IndexTx

            /* Prepare binding context and predicate. */
            val context = DefaultBindingContext()
            val columnBinding = context.bind(this.columns[0])
            val valueBinding = context.bindNull(Types.String)
            val predicate = BooleanPredicate.Atomic(ComparisonOperator.Binary.Equal(columnBinding, valueBinding), false)

            /* Check all entries. */
            for (entry in this.list.entries) {
                valueBinding.update(entry.key) /* Update value binding. */
                var found = false
                val cursor = indexTx.filter(predicate)
                while (cursor.moveNext() && !found) {
                    val rec = entityTx.read(cursor.key(), this.columns)
                    val id = rec[this.columns[0]] as StringValue
                    Assertions.assertEquals(entry.key, id)
                    if (entry.value.contains(rec[this.columns[1]])) {
                        found = true
                    }
                }
                cursor.close()
                Assertions.assertTrue(found)
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
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_EXCLUSIVE)
        val catalogueTx = txn.getCachedTxForDBO(this.catalogue) as CatalogueTx
        val schema = catalogueTx.schemaForName(this.schemaName)
        val schemaTx = txn.getCachedTxForDBO(schema) as SchemaTx
        val entity = schemaTx.entityForName(this.entityName)
        val entityTx = txn.getCachedTxForDBO(entity) as EntityTx
        val index = entityTx.indexForName(this.indexName)
        val indexTx = txn.getCachedTxForDBO(index) as IndexTx

        var count = 0
        val context = DefaultBindingContext()
        val predicate = BooleanPredicate.Atomic(ComparisonOperator.Binary.Equal(context.bind(this.columns[0]), context.bind(StringValue(UUID.randomUUID().toString()))), false)
        val cursor = indexTx.filter(predicate)
        cursor.forEach { count += 1 }
        cursor.close()
        Assertions.assertEquals(0, count)
        txn.commit()
    }

    /**
     * Generates and returns a new, random [StandaloneRecord] for inserting into the database.
     */
    override fun nextRecord(): StandaloneRecord {
        val id = StringValueGenerator.random(3)
        val value = LongValueGenerator.random(this.random)
        if (this.random.nextBoolean() && this.list.size <= 1000) {
            this.list.compute(id) { _, v ->
                val list = v ?: LinkedList()
                list.add(value)
                list
            }
        }
        return StandaloneRecord(0L, columns = this.columns, values = arrayOf(id, value))
    }
}