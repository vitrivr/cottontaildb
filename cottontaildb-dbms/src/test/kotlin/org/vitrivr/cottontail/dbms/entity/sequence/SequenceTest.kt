package org.vitrivr.cottontail.dbms.entity.sequence

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.generators.StringValueGenerator
import org.vitrivr.cottontail.dbms.entity.AbstractEntityTest
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.test.TestConstants
import java.util.*

class SequenceTest: AbstractEntityTest() {

    private val random = SplittableRandom()

    /** */
    override val entities: List<Pair<Name.EntityName, List<ColumnDef<*>>>> = listOf(
        TestConstants.TEST_ENTITY_NAME to listOf(
            ColumnDef(TestConstants.TEST_ENTITY_NAME.column(TestConstants.ID_COLUMN_NAME), Types.Int, nullable = false, primary = true, autoIncrement = true),
            ColumnDef(TestConstants.TEST_ENTITY_NAME.column(TestConstants.STRING_COLUMN_NAME), Types.String, nullable = false, primary = false, autoIncrement = false),
        )
    )

    @Test
    fun testSequence() {
        val txn1 = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx1 = DefaultQueryContext("test-sequence-insert", this.catalogue, txn1)

        /* Insert all entries. */
        val catalogueTx1 = this.catalogue.newTx(ctx1)
        val schema1 = catalogueTx1.schemaForName(this.schemaName)
        val schemaTx1 = schema1.newTx(ctx1)
        val entity1 = schemaTx1.entityForName(TestConstants.TEST_ENTITY_NAME)
        val entityTx1 = entity1.newTx(ctx1)
        repeat(TestConstants.TEST_COLLECTION_SIZE - 1) {
            val reference = this.nextRecord(it)
            entityTx1.insert(reference)
        }
        txn1.commit()

        /* Iterate over entries and read IDs. */
        val txn2 = this.manager.startTransaction(TransactionType.USER_READONLY)
        val ctx2 = DefaultQueryContext("test-sequence-read", this.catalogue, txn2)

        /* Insert all entries. */
        val catalogueTx2 = this.catalogue.newTx(ctx2)
        val schema2 = catalogueTx2.schemaForName(this.schemaName)
        val schemaTx2 = schema2.newTx(ctx2)
        val entity2 = schemaTx2.entityForName(TestConstants.TEST_ENTITY_NAME)
        val entityTx2 = entity2.newTx(ctx2)
        val cursor = entityTx2.cursor(this.entities[0].second.toTypedArray())
        for ((i, record) in cursor.withIndex()) {
            Assertions.assertEquals(i+1, (record[this.entities[0].second[0]] as? IntValue)!!.value)
        }
        txn2.commit()
    }

    /** We start with an empty entity. */
    override fun populateDatabase() {
       /* No op */
    }

    /**
     * Generates the next [StandaloneTuple] and returns it.
     */
    fun nextRecord(i: Int): StandaloneTuple {
        val string = StringValueGenerator.random(this.random)
        return StandaloneTuple(0L, columns = arrayOf(this.entities[0].second[1]), values = arrayOf(string))
    }
}