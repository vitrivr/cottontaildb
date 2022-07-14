package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty
import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.execution.operators.system.ExplainQueryOperator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.queries.binding.GrpcQueryBinder
import org.vitrivr.cottontail.dbms.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.dbms.queries.planning.rules.logical.*
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.BooleanIndexScanRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.FulltextIndexRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.NNSIndexScanClass1Rule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.NNSIndexScanClass3Rule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.merge.LimitingSortMergeRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.pushdown.CountPushdownRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.transform.DeferFetchOnPhysicalFetchRewriteRule
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.grpc.DQLGrpcKt
import kotlin.time.ExperimentalTime

/**
 * Implementation of [DQLGrpc.DQLImplBase], the gRPC endpoint for querying data in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.3.1
 */
@ExperimentalTime
class DQLService(override val catalogue: Catalogue, override val manager: TransactionManager) : DQLGrpcKt.DQLCoroutineImplBase(), TransactionalGrpcService {

    /** [CottontailQueryPlanner] used to generate execution plans from a logical query plan. */
    private val planner = CottontailQueryPlanner(
        logicalRules = listOf(
            LeftConjunctionRewriteRule,
            RightConjunctionRewriteRule,
            LeftConjunctionOnSubselectRewriteRule,
            RightConjunctionOnSubselectRewriteRule,
            DeferFetchOnScanRewriteRule,
            DeferFetchOnLogicalFetchRewriteRule
        ),
        physicalRules = listOf(
            BooleanIndexScanRule,
            NNSIndexScanClass1Rule,
            NNSIndexScanClass3Rule,
            FulltextIndexRule,
            CountPushdownRule,
            LimitingSortMergeRule,
            DeferFetchOnPhysicalFetchRewriteRule
        ),
        this.catalogue.config.cache.planCacheSize
    )

    /**
     * gRPC endpoint for executing queries.
     */
    override fun query(request: CottontailGrpc.QueryMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata, true) { ctx ->
        /* Bind query and create logical plan. */
        val canonical = GrpcQueryBinder.bind(request.query, ctx)
        ctx.assign(canonical)


        /* Plan query and create execution plan. */
        ctx.plan(this.planner)

        /* Generate operator tree. */
        ctx.toOperatorTree()
    }

    /**
     * gRPC endpoint for explaining queries.
     */
    override fun explain(request: CottontailGrpc.QueryMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata, true) { ctx ->
        /* Bind query and create canonical, logical plan. */
        val canonical = GrpcQueryBinder.bind(request.query, ctx)
        ctx.assign(canonical)

        /* Plan query and create execution plan. */
        val candidates = this.planner.plan(ctx)

        /* Generate operator tree. */
        ExplainQueryOperator(candidates, ctx.costPolicy)
    }

    /**
     * gRPC endpoint for handling PING requests.
     */
    override suspend fun ping(request: Empty): Empty {
        return Empty.getDefaultInstance()
    }
}
