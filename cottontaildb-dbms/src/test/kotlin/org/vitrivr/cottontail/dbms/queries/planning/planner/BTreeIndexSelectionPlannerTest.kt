package org.vitrivr.cottontail.dbms.queries.planning.planner

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.generators.StringValueGenerator
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
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
import org.vitrivr.cottontail.dbms.queries.planning.rules.logical.*
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.BooleanIndexScanRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.FulltextIndexRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.NNSIndexScanClass3Rule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.merge.LimitingSortMergeRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.pushdown.CountPushdownRule
import org.vitrivr.cottontail.dbms.schema.SchemaTx

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

    /** */
    override val collectionSize: Int
        get() = 5000

    /** List to use for IN queries. */
    private val inList = mutableListOf<StringValue>()

    /** The [CottontailQueryPlanner] used for this test. */
    private val planner = CottontailQueryPlanner(
        logicalRules = listOf(
            LeftConjunctionRewriteRule,
            RightConjunctionRewriteRule,
            LeftConjunctionOnSubselectRewriteRule,
            RightConjunctionOnSubselectRewriteRule,
            DeferFetchOnScanRewriteRule,
            DeferFetchOnLogicalFetchRewriteRule
        ),
        physicalRules = listOf(BooleanIndexScanRule, NNSIndexScanClass3Rule, FulltextIndexRule, CountPushdownRule, LimitingSortMergeRule),
        this.catalogue.config.cache.planCacheSize
    )

    /**
     * Tests the [BooleanIndexScanRule] in case of an EQUALS comparison.
     */
    @Test
    fun testEqualsWithoutHint() {
        for (i in 0 until 1000) {
            val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_READONLY)
            try {
                val ctx = DefaultQueryContext("test", this.catalogue, txn)
                val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
                val schema = catalogueTx.schemaForName(this.schemaName)
                val schemaTx = txn.getTx(schema) as SchemaTx
                val entity = schemaTx.entityForName(this.entityName)
                val entityTx = txn.getTx(entity) as EntityTx
                val bindings = this.columns.map { ctx.bindings.bind(it) to it }

                /* Bind EQUALS operator. */
                val op = ComparisonOperator.Binary.Equal(bindings[0].first, ctx.bindings.bind(this.inList[this.random.nextInt(0, this.inList.size - 1)]))

                /* Prepare simple scan with projection. */
                val scan0 = EntityScanLogicalOperatorNode(0, entityTx, bindings)
                val filter0 = FilterLogicalOperatorNode(scan0, BooleanPredicate.Atomic(op, false))
                val projection0 = SelectProjectionLogicalOperatorNode(filter0, listOf(this.columns[0].name, this.columns[1].name))

                /* Execute query planing. */
                ctx.assign(projection0)
                ctx.plan(this.planner)

                /* Check if index scan was selected. */
                Assertions.assertTrue(ctx.physical?.base?.firstOrNull() is IndexScanPhysicalOperatorNode)
                Assertions.assertEquals(this.indexName, (ctx.physical!!.base.first() as IndexScanPhysicalOperatorNode).index.dbo.name)
                Assertions.assertEquals(this.indexType, (ctx.physical!!.base.first() as IndexScanPhysicalOperatorNode).index.dbo.type)
            } finally {
                txn.rollback()
            }

            /* Add more records. */
            this.populateDatabase()
        }
    }

    /**
     * Tests the [BooleanIndexScanRule] in case of an EQUALS comparison when the [QueryHint.NoIndex] hint is set.
     */
    @Test
    fun testEqualsWithNoIndexHint() {
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_READONLY)
        try {
            val ctx = DefaultQueryContext("test", this.catalogue, txn, setOf(QueryHint.IndexHint.None))
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = txn.getTx(entity) as EntityTx
            val bindings = this.columns.map { ctx.bindings.bind(it) to it }

            /* Bind EQUALS operator. */
            val op = ComparisonOperator.Binary.Equal(bindings[0].first, ctx.bindings.bind(this.inList[this.random.nextInt(0, this.inList.size - 1)]))

            /* Prepare simple scan with projection. */
            val scan0 = EntityScanLogicalOperatorNode(0, entityTx, bindings)
            val filter0 = FilterLogicalOperatorNode(scan0, BooleanPredicate.Atomic(op, false))
            val projection0 = SelectProjectionLogicalOperatorNode(filter0, listOf(this.columns[0].name, this.columns[1].name))

            /* Execute query planing. */
            ctx.assign(projection0)
            ctx.plan(this.planner)

            /* Check if index scan was selected. */
            Assertions.assertTrue(ctx.physical?.base?.firstOrNull() is EntityScanPhysicalOperatorNode)
        } finally {
            txn.rollback()
        }
    }


    /**
     * Tests the [BooleanIndexScanRule] in case of an IN comparison.
     */
    @Test
    fun testInWithoutHint() {
        for (i in 0 until 1000) {
            val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_READONLY)
            try {
                val ctx = DefaultQueryContext("test", this.catalogue, txn)
                val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
                val schema = catalogueTx.schemaForName(this.schemaName)
                val schemaTx = txn.getTx(schema) as SchemaTx
                val entity = schemaTx.entityForName(this.entityName)
                val entityTx = txn.getTx(entity) as EntityTx
                val bindings = this.columns.map { ctx.bindings.bind(it) to it }

                /* Bind IN operator. */
                val op = ComparisonOperator.In(bindings[0].first, this.inList.map { ctx.bindings.bind(it) })

                /* Prepare simple scan with projection. */
                val scan0 = EntityScanLogicalOperatorNode(0, entityTx, bindings)
                val filter0 = FilterLogicalOperatorNode(scan0, BooleanPredicate.Atomic(op, false))
                val projection0 = SelectProjectionLogicalOperatorNode(filter0, listOf(this.columns[0].name, this.columns[1].name))

                /* Execute query planing. */
                ctx.assign(projection0)
                ctx.plan(this.planner)

                /* Check if index scan was selected. */
                Assertions.assertTrue(ctx.physical?.base?.firstOrNull() is IndexScanPhysicalOperatorNode)
                Assertions.assertEquals(this.indexName, (ctx.physical!!.base.first() as IndexScanPhysicalOperatorNode).index.dbo.name)
                Assertions.assertEquals(this.indexType, (ctx.physical!!.base.first() as IndexScanPhysicalOperatorNode).index.dbo.type)
            } finally {
                txn.rollback()
            }
            /* Add more records. */
            this.populateDatabase()
        }
    }

    /**
     * Tests the [BooleanIndexScanRule] in case of an IN comparison.
     */
    @Test
    fun testInWithoutHintButWithAndCondition() {
        for (i in 0 until 1000) {
            val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_READONLY)
            try {
                val ctx = DefaultQueryContext("test", this.catalogue, txn)
                val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
                val schema = catalogueTx.schemaForName(this.schemaName)
                val schemaTx = txn.getTx(schema) as SchemaTx
                val entity = schemaTx.entityForName(this.entityName)
                val entityTx = txn.getTx(entity) as EntityTx
                val bindings = this.columns.map { ctx.bindings.bind(it) to it }

                /* Bind IN operator. */
                val op1 = ComparisonOperator.In(bindings[0].first, this.inList.map { ctx.bindings.bind(it) })
                val op2 = ComparisonOperator.Binary.Less(bindings[1].first, ctx.bindings.bind(LongValue.ZERO))

                /* Prepare simple scan with projection. */
                val scan0 = EntityScanLogicalOperatorNode(0, entityTx, bindings)
                val filter0 = FilterLogicalOperatorNode(scan0, BooleanPredicate.Compound.And(BooleanPredicate.Atomic(op1, false), BooleanPredicate.Atomic(op2, false)))
                val projection0 = SelectProjectionLogicalOperatorNode(filter0, listOf(this.columns[0].name, this.columns[1].name))

                /* Execute query planing. */
                ctx.assign(projection0)
                ctx.plan(this.planner)

                /* Check if index scan was selected. */
                Assertions.assertTrue(ctx.physical?.base?.firstOrNull() is IndexScanPhysicalOperatorNode)
                Assertions.assertEquals(this.indexName, (ctx.physical!!.base.first() as IndexScanPhysicalOperatorNode).index.dbo.name)
                Assertions.assertEquals(this.indexType, (ctx.physical!!.base.first() as IndexScanPhysicalOperatorNode).index.dbo.type)
            } finally {
                txn.rollback()
            }
            /* Add more records. */
            this.populateDatabase()
        }
    }

    /**
     * Tests the [BooleanIndexScanRule] in case of an IN comparison.
     */
    @Test
    fun testInWithNoIndexHint() {
        val txn = this.manager.TransactionImpl(TransactionType.SYSTEM_READONLY)
        try {
            val ctx = DefaultQueryContext("test", this.catalogue, txn, setOf(QueryHint.IndexHint.None))
            val catalogueTx = txn.getTx(this.catalogue) as CatalogueTx
            val schema = catalogueTx.schemaForName(this.schemaName)
            val schemaTx = txn.getTx(schema) as SchemaTx
            val entity = schemaTx.entityForName(this.entityName)
            val entityTx = txn.getTx(entity) as EntityTx
            val bindings = this.columns.map { ctx.bindings.bind(it) to it }

            /* Bind IN operator. */
            val op = ComparisonOperator.In(bindings[0].first, this.inList.map { ctx.bindings.bind(it) })

            /* Prepare simple scan with projection. */
            val scan0 = EntityScanLogicalOperatorNode(0, entityTx, bindings)
            val filter0 = FilterLogicalOperatorNode(scan0, BooleanPredicate.Atomic(op, false))
            val projection0 = SelectProjectionLogicalOperatorNode(filter0, listOf(this.columns[0].name, this.columns[1].name))

            /* Execute query planing. */
            ctx.assign(projection0)
            ctx.plan(this.planner)

            /* Check if index scan was selected. */
            Assertions.assertTrue(ctx.physical?.base?.firstOrNull() is EntityScanPhysicalOperatorNode)
        } finally {
            txn.rollback()
        }
    }

    /**
     * Generates and returns a new, random [StandaloneRecord] for inserting into the database.
     */
    override fun nextRecord(): StandaloneRecord {
        val size = this.random.nextInt(10, 25)
        val id = StringValueGenerator.random(size)
        val value = LongValue(this.random.nextLong(-100000L, 10000L))
        if (this.inList.size < 50000 && this.random.nextFloat() <= 0.1f) {
            this.inList.add(id)
        }
        return StandaloneRecord(0L, columns = this.columns, values = arrayOf(id, value))
    }
}