package org.vitrivr.cottontail.server.grpc.services

import com.google.protobuf.Empty
import kotlinx.coroutines.flow.Flow
import org.vitrivr.cottontail.dbms.execution.operators.system.ExplainQueryOperator
import org.vitrivr.cottontail.dbms.queries.QueryHint
import org.vitrivr.cottontail.dbms.queries.binding.GrpcQueryBinder
import org.vitrivr.cottontail.dbms.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.dbms.queries.planning.rules.logical.LeftConjunctionRewriteRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.logical.RightConjunctionRewriteRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.*
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.pushdown.CountPushdownRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.simd.FunctionVectorisationRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.sort.ExternalSortRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.sort.LimitingSortMergeRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.transform.DeferFunctionRewriteRule
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DQLGrpc
import org.vitrivr.cottontail.grpc.DQLGrpcKt
import org.vitrivr.cottontail.server.Instance
import kotlin.time.ExperimentalTime

/**
 * Implementation of [DQLGrpc.DQLImplBase], the gRPC endpoint for querying data in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.5.0
 */
@ExperimentalTime
class DQLService(override val instance: Instance) : DQLGrpcKt.DQLCoroutineImplBase(), TransactionalGrpcService {

    /** [CottontailQueryPlanner] used to generate execution plans from a logical query plan. */
    private val planner: CottontailQueryPlanner

    init {
        val logical = listOf(
            LeftConjunctionRewriteRule,
            RightConjunctionRewriteRule
        )
        val physical =  mutableListOf(
            BooleanIndexScanRule,
            BooleanIntersectionScanRule,
            NNSIndexScanClass1Rule,
            NNSIndexScanClass3Rule,
            FulltextIndexRule,
            CountPushdownRule,
            ExternalSortRule,
            LimitingSortMergeRule,
            DeferFunctionRewriteRule
        )
        if (this.catalogue.config.execution.simd) {
            physical += FunctionVectorisationRule(this.catalogue.config.execution.simdThreshold)
        }
        this.planner = CottontailQueryPlanner(logical, physical, this.catalogue.config.cache.planCacheSize)
    }

    /**
     * gRPC endpoint for executing queries.
     */
    override fun query(request: CottontailGrpc.QueryMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata, true) { ctx ->
        /* Bind query and create logical plan. */
        with(ctx) {
            val canonical = GrpcQueryBinder.bind(request.query)
            ctx.register(canonical)

            /* Plan and/or implement query to create execution plan. */
            if (ctx.hints.contains(QueryHint.NoOptimisation)) {
                ctx.implement()
            } else {
                ctx.plan(this@DQLService.planner, false, true)
            }

            /* Generate operator tree. */
            ctx.toOperatorTree()
        }
    }

    /**
     * gRPC endpoint for explaining queries.
     */
    override fun explain(request: CottontailGrpc.QueryMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata, true) { ctx ->
        /* Bind query and create canonical, logical plan. */
        with(ctx) {
            val canonical = GrpcQueryBinder.bind(request.query)
            ctx.register(canonical)

            /* Plan query and create execution plan. */
            val candidates = with(ctx) {
               this@DQLService.planner.plan(ctx.logical.first(), limit = 5)
            }

            /* Generate operator tree. */
            ExplainQueryOperator(candidates, ctx)
        }
    }

    /**
     * gRPC endpoint for handling PING requests.
     */
    override suspend fun ping(request: Empty): Empty {
        return Empty.getDefaultInstance()
    }
}
