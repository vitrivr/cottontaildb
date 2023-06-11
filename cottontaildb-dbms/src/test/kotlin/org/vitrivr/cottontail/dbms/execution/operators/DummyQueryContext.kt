package org.vitrivr.cottontail.dbms.execution.operators

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.functions.FunctionRegistry
import org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.queries.QueryHint
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.test.TestConstants

/**
 * A dummy [QueryContext] that can be used for testing operators.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DummyQueryContext: QueryContext {
    override val queryId: String = "test"
    override val catalogue: Catalogue = object : Catalogue {
        override val config: Config = TestConstants.testConfig()
        override val functions: FunctionRegistry = FunctionRegistry()
        override val name = Name.RootName
        override val parent: DBO? = null
        override val catalogue: Catalogue
            get() = this
        override val version: DBOVersion
            get() = DBOVersion.UNDEFINED
        override fun newTx(context: QueryContext): CatalogueTx = throw UnsupportedOperationException("Operation not supported for dummy catalogue.")
        override fun close() {}
    }
    override val txn: Transaction
        get() = throw UnsupportedOperationException("Operation not supported for dummy query context.")
    override val bindings: BindingContext
        get() = throw UnsupportedOperationException("Operation not supported for dummy query context.")
    override val hints: Set<QueryHint> = emptySet()
    override val costPolicy: CostPolicy = this.catalogue.config.cost
    override val logical: List<OperatorNode.Logical> = emptyList()
    override val physical: List<OperatorNode.Physical> = emptyList()
    override val output: List<ColumnDef<*>> = emptyList()
    override val order: List<Pair<ColumnDef<*>, SortOrder>> = emptyList()
    override fun nextGroupId(): GroupId = 0
    override fun register(plan: OperatorNode.Logical) = throw UnsupportedOperationException("Operation not supported for dummy query context.")
    override fun register(plan: OperatorNode.Physical) = throw UnsupportedOperationException("Operation not supported for dummy query context.")
    override fun plan(planner: CottontailQueryPlanner, bypassCache: Boolean, cache: Boolean) = throw UnsupportedOperationException("Operation not supported for dummy query context.")
    override fun implement() = throw UnsupportedOperationException("Operation not supported for dummy query context.")
    override fun split(): QueryContext = throw UnsupportedOperationException("Operation not supported for dummy query context.")
    override fun toOperatorTree(): Operator =throw UnsupportedOperationException("Operation not supported for dummy query context.")
}