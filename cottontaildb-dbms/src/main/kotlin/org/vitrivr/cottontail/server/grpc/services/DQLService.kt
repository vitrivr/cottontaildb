package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty
import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.execution.operators.system.ExplainQueryOperator
import org.vitrivr.cottontail.dbms.queries.binding.GrpcQueryBinder
import org.vitrivr.cottontail.dbms.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.dbms.queries.planning.rules.logical.*
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.BooleanIndexScanRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.FulltextIndexRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.NNSIndexScanRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.merge.LimitingSortMergeRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.pushdown.CountPushdownRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.simd.SIMDRule
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.grpc.DQLGrpcKt
import kotlin.time.ExperimentalTime

/**
 * Implementation of [DQLGrpc.DQLImplBase], the gRPC endpoint for querying data in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
@ExperimentalTime
class DQLService(override val catalogue: Catalogue, override val manager: org.vitrivr.cottontail.dbms.execution.TransactionManager) : DQLGrpcKt.DQLCoroutineImplBase(), TransactionalGrpcService {

    /** [CottontailQueryPlanner] used to generate execution plans from a logical query plan. */
    private val planner = CottontailQueryPlanner(
        logicalRules = listOf(
            LeftConjunctionRewriteRule,
            RightConjunctionRewriteRule,
            LeftConjunctionOnSubselectRewriteRule,
            RightConjunctionOnSubselectRewriteRule,
            DeferFetchOnScanRewriteRule,
            DeferFetchOnFetchRewriteRule
        ),
        //TODO @Colin add your newly implemented rule here
        physicalRules = listOf(BooleanIndexScanRule, NNSIndexScanRule, FulltextIndexRule, CountPushdownRule, LimitingSortMergeRule, SIMDRule),
        this.catalogue.config.cache.planCacheSize
    )

    /**
     * gRPC endpoint for executing queries.
     */
    override fun query(request: CottontailGrpc.QueryMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata) { ctx ->
        /* Bind query and create logical plan. */
        GrpcQueryBinder.bind(request.query, ctx)

        /* Plan query and create execution plan. */
        this.planner.planAndSelect(ctx)

        /* Generate operator tree. */
        ctx.toOperatorTree()
    }

    /**
     * gRPC endpoint for explaining queries.
     */
    override fun explain(request: CottontailGrpc.QueryMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata) { ctx ->
        /* Bind query and create logical plan. */
        GrpcQueryBinder.bind(request.query, ctx)

        /* Plan query and create execution plan. */
        val candidates = this.planner.plan(ctx)

        /* Generate operator tree. */
        ExplainQueryOperator(candidates)
    }

    /**
     * gRPC endpoint for handling PING requests.
     */
    override suspend fun ping(request: Empty): Empty {
        return Empty.getDefaultInstance()
    }
}
