package org.vitrivr.cottontail.dbms.queries.planning.planner

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.generators.StringValueGenerator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionType
import org.vitrivr.cottontail.dbms.index.AbstractIndexTest
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.QueryHint
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.projection.SelectProjectionLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.sources.EntityScanLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.IndexScanPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.dbms.queries.planning.rules.logical.LeftConjunctionRewriteRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.logical.RightConjunctionRewriteRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.BooleanIndexScanRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.FulltextIndexRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.NNSIndexScanClass3Rule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.pushdown.CountPushdownRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.sort.LimitingSortMergeRule

/**
 * A collection of test cases that test the outcome for index selection in presence of an [IndexType.BTREE].
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class BTreeIndexSelectionPlannerTest : AbstractIndexTest() {

    /** [ColumnDef] in test entity. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.String),
        ColumnDef(this.entityName.column("longValue"), Types.Long)
    )

    /** The [ColumnDef] that is being indexed. */
    override val indexColumn: ColumnDef<*>
        get() = this.columns[0]

    /** The [Name.IndexName] of the [Index] used for this test. */
    override val indexName: Name.IndexName
        get() = this.entityName.index("idx_id")

    /** The [IndexType] of the [Index] used for this test. */
    override val indexType: IndexType
        get() = IndexType.BTREE

    /** List to use for IN queries. */
    private val inList = mutableListOf<StringValue>()

    /** The [CottontailQueryPlanner] used for this test. */
    private val planner = CottontailQueryPlanner(
        logicalRules = listOf(
            LeftConjunctionRewriteRule,
            RightConjunctionRewriteRule
        ),
        physicalRules = listOf(
            BooleanIndexScanRule,
            NNSIndexScanClass3Rule,
            FulltextIndexRule,
            CountPushdownRule,
            LimitingSortMergeRule,
            DeferFetchOnScanRewriteRule,
            DeferFetchOnFetchRewriteRule
        ),
        this.catalogue.config.cache.planCacheSize
    )

    /**
     * Tests the [BooleanIndexScanRule] in case of an EQUALS comparison.
     */
    @Test
    fun testEqualsWithoutHint() {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_READONLY)
        try {
            val ctx = DefaultQueryContext("index-test", this.catalogue, txn)
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(ctx)
            val bindings = this.columns.map { ctx.bindings.bind(it, it)}

            /* Bind EQUALS operator. */
            val op = ComparisonOperator.Equal(bindings[0], ctx.bindings.bind(this.inList[this.random.nextInt(0, this.inList.size - 1)]))

            /* Prepare simple scan with projection. */
            val scan0 = EntityScanLogicalOperatorNode(0, entityTx, bindings)
            val filter0 = FilterLogicalOperatorNode(scan0, BooleanPredicate.Comparison(op))
            val projection0 = SelectProjectionLogicalOperatorNode(filter0, listOf(bindings[0], bindings[1]))

            /* Execute query planing. */
            ctx.register(projection0)
            ctx.plan(this.planner)

            /* Check if index scan was selected. */
            Assertions.assertTrue(ctx.physical.firstOrNull()?.base?.firstOrNull() is IndexScanPhysicalOperatorNode)
            Assertions.assertEquals(this.indexName, (ctx.physical.first().base.first() as IndexScanPhysicalOperatorNode).tx.dbo.name)
            Assertions.assertEquals(this.indexType, (ctx.physical.first().base.first() as IndexScanPhysicalOperatorNode).tx.dbo.type)
        } finally {
            txn.abort()
        }
    }

    /**
     * Tests the [BooleanIndexScanRule] in case of an EQUALS comparison when the [QueryHint.IndexHint.None] hint is set.
     */
    @Test
    fun testEqualsWithNoIndexHint() {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_READONLY)
        try {
            val ctx = DefaultQueryContext("test", this.catalogue, txn, setOf(QueryHint.IndexHint.None))
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(ctx)
            val bindings = this.columns.map { ctx.bindings.bind(it, it) }

            /* Bind EQUALS operator. */
            val op = ComparisonOperator.Equal(bindings[0], ctx.bindings.bind(this.inList[this.random.nextInt(0, this.inList.size - 1)]))

            /* Prepare simple scan with projection. */
            val scan0 = EntityScanLogicalOperatorNode(0, entityTx, bindings)
            val filter0 = FilterLogicalOperatorNode(scan0, BooleanPredicate.Comparison(op))
            val projection0 = SelectProjectionLogicalOperatorNode(filter0, listOf(bindings[0], bindings[1]))

            /* Execute query planing. */
            ctx.register(projection0)
            ctx.plan(this.planner)

            /* Check if index scan was selected. */
            Assertions.assertTrue(ctx.physical.firstOrNull()?.base?.firstOrNull() is EntityScanPhysicalOperatorNode)
        } finally {
            txn.abort()
        }
    }


    /**
     * Tests the [BooleanIndexScanRule] in case of an IN comparison.
     */
    @Test
    fun testInWithoutHint() {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_READONLY)
        try {
            val ctx = DefaultQueryContext("test", this.catalogue, txn)
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(ctx)
            val bindings = this.columns.map { ctx.bindings.bind(it, it) }

            /* Bind IN operator. */
            val op = ComparisonOperator.In(bindings[0], ctx.bindings.bind(this.inList))

            /* Prepare simple scan with projection. */
            val scan0 = EntityScanLogicalOperatorNode(0, entityTx, bindings)
            val filter0 = FilterLogicalOperatorNode(scan0, BooleanPredicate.Comparison(op))
            val projection0 = SelectProjectionLogicalOperatorNode(filter0, listOf(bindings[0], bindings[1]))

            /* Execute query planing. */
            ctx.register(projection0)
            ctx.plan(this.planner)

            /* Check if index scan was selected. */
            Assertions.assertTrue(ctx.physical.firstOrNull()?.base?.firstOrNull() is IndexScanPhysicalOperatorNode)
            Assertions.assertEquals(this.indexName, (ctx.physical.first().base.first() as IndexScanPhysicalOperatorNode).tx.dbo.name)
            Assertions.assertEquals(this.indexType, (ctx.physical.first().base.first() as IndexScanPhysicalOperatorNode).tx.dbo.type)
        } finally {
            txn.abort()
        }
        /* Add more records. */
        this.populateDatabase()
    }

    /**
     * Tests the [BooleanIndexScanRule] in case of an IN comparison.
     */
    @Test
    fun testInWithoutHintButWithAndCondition() {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_READONLY)
        try {
            val ctx = DefaultQueryContext("test", this.catalogue, txn)
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(ctx)
            val bindings = this.columns.map { ctx.bindings.bind(it, it)}

            /* Bind IN operator. */
            val op1 = ComparisonOperator.In(bindings[0], ctx.bindings.bind(this.inList))
            val op2 = ComparisonOperator.Less(bindings[1], ctx.bindings.bind(LongValue.ZERO))

            /* Prepare simple scan with projection. */
            val scan0 = EntityScanLogicalOperatorNode(0, entityTx, bindings)
            val filter0 = FilterLogicalOperatorNode(scan0, BooleanPredicate.And(BooleanPredicate.Comparison(op1), BooleanPredicate.Comparison(op2)))
            val projection0 = SelectProjectionLogicalOperatorNode(filter0, listOf(bindings[0], bindings[1]))

            /* Execute query planing. */
            ctx.register(projection0)
            ctx.plan(this.planner)

            /* Check if index scan was selected. */
            Assertions.assertTrue(ctx.physical.firstOrNull()?.base?.firstOrNull() is IndexScanPhysicalOperatorNode)
            Assertions.assertEquals(this.indexName, (ctx.physical.first().base.first() as IndexScanPhysicalOperatorNode).tx.dbo.name)
            Assertions.assertEquals(this.indexType, (ctx.physical.first().base.first() as IndexScanPhysicalOperatorNode).tx.dbo.type)
        } finally {
            txn.abort()
        }
    }

    /**
     * Tests the [BooleanIndexScanRule] in case of an IN comparison.
     */
    @Test
    fun testInWithNoIndexHint() {
        val txn = this.manager.startTransaction(TransactionType.SYSTEM_READONLY)
        try {
            val ctx = DefaultQueryContext("test", this.catalogue, txn, setOf(QueryHint.IndexHint.None))
            val catalogueTx = this.catalogue.createOrResumeTx(ctx)
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = schema.newTx(ctx)
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = entity.createOrResumeTx(ctx)
            val bindings = this.columns.map { ctx.bindings.bind(it, it)}

            /* Bind IN operator. */
            val op = ComparisonOperator.In(bindings[0], ctx.bindings.bind(this.inList))

            /* Prepare simple scan with projection. */
            val scan0 = EntityScanLogicalOperatorNode(0, entityTx, bindings)
            val filter0 = FilterLogicalOperatorNode(scan0, BooleanPredicate.Comparison(op))
            val projection0 = SelectProjectionLogicalOperatorNode(filter0, listOf(bindings[0], bindings[1]))

            /* Execute query planing. */
            ctx.register(projection0)
            ctx.plan(this.planner)

            /* Check if index scan was selected. */
            Assertions.assertTrue(ctx.physical.firstOrNull()?.base?.firstOrNull() is EntityScanPhysicalOperatorNode)
        } finally {
            txn.abort()
        }
    }

    /**
     * Generates and returns a new, random [StandaloneTuple] for inserting into the database.
     */
    override fun nextRecord(): StandaloneTuple {
        val size = this.random.nextInt(10, 25)
        val id = StringValueGenerator.random(size)
        val value = LongValue(this.random.nextLong(-100000L, 10000L))
        if (this.inList.size < 50000 && this.random.nextFloat() <= 0.1f) {
            this.inList.add(id)
        }
        return StandaloneTuple(0L, columns = this.columns, values = arrayOf(id, value))
    }
}