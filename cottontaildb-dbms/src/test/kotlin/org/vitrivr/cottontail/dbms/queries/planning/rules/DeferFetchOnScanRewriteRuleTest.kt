package org.vitrivr.cottontail.dbms.queries.planning.rules

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.dbms.entity.AbstractEntityTest
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.queries.binding.DefaultBindingContext
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.projection.SelectProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntitySamplePhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.FetchPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.transform.DeferFetchOnFetchRewriteRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.transform.DeferFetchOnScanRewriteRule

/**
 * A collection of test cases for the [DeferFetchOnScanRewriteRule].
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class DeferFetchOnScanRewriteRuleTest : AbstractEntityTest() {

    /** [Name.EntityName] of test entity. */
    private val entityName = this.schemaName.entity("test-entity")

    /** [ColumnDef] in test entity. */
    private val columns = listOf(
        ColumnDef(this.entityName.column("id"), Types.Long),
        ColumnDef(this.entityName.column("doubleValue"), Types.Double),
        ColumnDef(this.entityName.column("stringValue"), Types.String),
        ColumnDef(this.entityName.column("intValue"), Types.Int),
        ColumnDef(this.entityName.column("booleanValue"), Types.Boolean)
    )

    /** List of entities that should be prepared for this test. */
    override val entities: List<Pair<Name.EntityName, List<ColumnDef<*>>>> = listOf(
        this.entityName to this.columns
    )


    /**
     * Makes a basic test whether [DeferFetchOnScanRewriteRule.canBeApplied] works as expected and generates output accordingly.
     */
    @Test
    fun testNoMatch() {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val ctx = DefaultQueryContext("test", this.catalogue, txn)
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(ctx)
            val bindings = this.columns.map { ctx.bindings.bind(it, it) }

            /* Prepare simple SAMPLE with projection. */
            val sample0 = EntitySamplePhysicalOperatorNode(0, entityTx, bindings, 0.5f)
            SelectProjectionPhysicalOperatorNode(sample0, bindings)

            /* Check DeferFetchOnFetchRewriteRule.canBeApplied and test output for null. */
            Assertions.assertFalse(DeferFetchOnScanRewriteRule.canBeApplied(sample0, ctx))
            Assertions.assertThrows(IllegalArgumentException::class.java) {
                DeferFetchOnFetchRewriteRule.apply(sample0, ctx)
            }
        } finally {
            txn.abort()
        }
    }

    /**
     * Tests case of no deferral because all columns are selected.
     */
    @Test
    fun testNoDefer() {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val ctx = DefaultQueryContext("test", this.catalogue, txn)
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(ctx)
            val bindings = this.columns.map { ctx.bindings.bind(it, it) }

            /* Prepare simple SAMPLE with projection. */
            val scan0 = EntityScanPhysicalOperatorNode(0, entityTx, bindings)
            SelectProjectionPhysicalOperatorNode(scan0, bindings)

            /* Step 1: Execute DeferFetchOnScanRewriteRule and make basic assertions. */
            Assertions.assertTrue(DeferFetchOnScanRewriteRule.canBeApplied(scan0, ctx))
            val result1 = DeferFetchOnScanRewriteRule.apply(scan0, ctx)

            /* Output should be null because no deferral can take place. */
            Assertions.assertEquals(null, result1)
        } finally {
            txn.abort()
        }
    }

    /**
     * Tests simple deferral and dropping of columns in a SELECT followed by a PROJECTION
     */
    @Test
    fun testDeferAndDrop() {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val ctx = DefaultQueryContext("test", this.catalogue, txn)
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(ctx)
            val bindings = this.columns.map { ctx.bindings.bind(it, it) }

            /* Prepare simple scan with projection. */
            val scan0 = EntityScanPhysicalOperatorNode(0, entityTx, bindings)
            val projection0 = SelectProjectionPhysicalOperatorNode(scan0, listOf(bindings[0], bindings[1]))

            /* Execute rule, */
            Assertions.assertTrue(DeferFetchOnScanRewriteRule.canBeApplied(scan0, ctx))
            val result1 = DeferFetchOnScanRewriteRule.apply(scan0, ctx)

            /* Check order: SCAN -> PROJECT. */
            Assertions.assertTrue(result1 is SelectProjectionPhysicalOperatorNode)
            val scan1 = (result1 as SelectProjectionPhysicalOperatorNode).input
            Assertions.assertTrue(scan1 is EntityScanPhysicalOperatorNode)

            /* Check that columns are preserved and unnecessary columns are dropped. */
            Assertions.assertTrue(result1.columns == projection0.columns) /* Columns of the resulting PROJECTION should be the same as the original PROJECTION */
            Assertions.assertTrue((result1.input as EntityScanPhysicalOperatorNode).columns == projection0.columns) /* Columns SCANNED should only contain the projected columns. */
        } finally {
            txn.abort()
        }
    }

    /**
     * Tests simple deferral and dropping of columns in a SELECT followed by a PROJECTION
     */
    @Test
    fun testDeferAfterFilter() {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        try {
            val ctx = DefaultQueryContext("test", this.catalogue, txn)
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(ctx)
            val bindings = this.columns.map { ctx.bindings.bind(it, it) }

            /* Prepare simple SCAN followed by a FILTER, followed by a PROJECTION. */
            val context = DefaultBindingContext()
            val scan0 = EntityScanPhysicalOperatorNode(0, entityTx, bindings)
            val filter0 = FilterPhysicalOperatorNode(scan0, BooleanPredicate.Comparison(ComparisonOperator.Equal(bindings[2], context.bindNull(this.columns[2].type))))
            val projection0 = SelectProjectionPhysicalOperatorNode(filter0, listOf(bindings[0], bindings[1]))


            /* Execute rule and make basic assertions. */
            Assertions.assertTrue(DeferFetchOnScanRewriteRule.canBeApplied(scan0, ctx))
            val result1 = DeferFetchOnScanRewriteRule.apply(scan0, ctx)

            /* Check order: SCAN -> FILTER -> FETCH -> PROJECT. */
            Assertions.assertTrue(result1 is SelectProjectionPhysicalOperatorNode)
            Assertions.assertTrue(result1!!.columns == projection0.columns)
            val fetch1 = (result1 as SelectProjectionPhysicalOperatorNode).input
            Assertions.assertTrue(fetch1 is FetchPhysicalOperatorNode)
            val filter1 = (fetch1 as FetchPhysicalOperatorNode).input
            Assertions.assertTrue(filter1 is FilterPhysicalOperatorNode)
            val scan1 = (filter1 as FilterPhysicalOperatorNode).input
            Assertions.assertTrue(scan1 is EntityScanPhysicalOperatorNode)

            /* Check that columns are preserved and unnecessary columns are dropped. */
            Assertions.assertTrue(result1.columns == projection0.columns) /* Columns of the resulting PROJECTION should be the same as the original PROJECTION */

            val combined = ((scan1 as EntityScanPhysicalOperatorNode).columns + fetch1.fetch) /* Columns of the SCAN + FETCH. */
            Assertions.assertEquals(scan0.columns.size, combined.size) /* Columns FETCHED + columns SCANNED should of the same size as the SCAN columns. */
            Assertions.assertTrue(scan1.columns.all { combined.contains(it) }) /* Columns FETCHED + columns SCANNED should be all columns in the original SCAN. */
            Assertions.assertTrue(scan1.columns == filter0.predicate.columns.toList()) /* Columns SCANNED should only contain the columns used by FILTER. */
        } finally {
            txn.abort()
        }
    }

    /**
     * We don't need data for this test.
     */
    override fun populateDatabase() {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_EXCLUSIVE)
        val ctx = DefaultQueryContext("populate-database", this.catalogue, txn)
        try {
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(ctx)

            /* Insert data and track how many entries have been stored for the test later. */
            entityTx.insert(StandaloneTuple(1L, this.columns.toTypedArray(), arrayOf(LongValue(1L), DoubleValue(0.0), StringValue("test"), IntValue(1), BooleanValue(true))))
            entityTx.insert(StandaloneTuple(2L, this.columns.toTypedArray(), arrayOf(LongValue(2L), DoubleValue(1.0), StringValue("test"), IntValue(2), BooleanValue(false))))

            txn.commit()
        } catch (e: Throwable) {
            txn.abort()
            throw e
        }
    }
}