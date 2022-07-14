package org.vitrivr.cottontail.server.grpc.services

import kotlinx.coroutines.flow.single
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.queries.binding.GrpcQueryBinder
import org.vitrivr.cottontail.dbms.queries.planning.CottontailQueryPlanner
import org.vitrivr.cottontail.dbms.queries.planning.rules.logical.DeferFetchOnLogicalFetchRewriteRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.logical.DeferFetchOnScanRewriteRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.logical.LeftConjunctionRewriteRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.logical.RightConjunctionRewriteRule
import org.vitrivr.cottontail.dbms.queries.planning.rules.physical.index.BooleanIndexScanRule
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DMLGrpc
import org.vitrivr.cottontail.grpc.DMLGrpcKt
import kotlin.time.ExperimentalTime

/**
 * Implementation of [DMLGrpc.DMLImplBase], the gRPC endpoint for inserting data into Cottontail DB [DefaultEntity]s.
 *
 * @author Ralph Gasser
 * @version 2.3.1
 */
@ExperimentalTime
class DMLService(override val catalogue: Catalogue, override val manager: TransactionManager) : DMLGrpcKt.DMLCoroutineImplBase(), TransactionalGrpcService {

    /** [CottontailQueryPlanner] instance used to generate execution plans from query definitions. */
    private val planner = CottontailQueryPlanner(
        logicalRules = listOf(
            LeftConjunctionRewriteRule,
            RightConjunctionRewriteRule,
            DeferFetchOnScanRewriteRule,
            DeferFetchOnLogicalFetchRewriteRule
        ),
        physicalRules = listOf(BooleanIndexScanRule),
        this.catalogue.config.cache.planCacheSize
    )

    /**
     * gRPC endpoint for handling UPDATE queries.
     */
    override suspend fun update(request: CottontailGrpc.UpdateMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        /* Bind query and create logical plan. */
        val canonical = GrpcQueryBinder.bind(request, ctx)
        ctx.assign(canonical)

        /* Plan query and create execution plan. */
        ctx.plan(this.planner)

        /* Generate operator tree. */
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for handling DELETE queries.
     */
    override suspend fun delete(request: CottontailGrpc.DeleteMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        /* Bind query and create logical plan. */
        val canonical = GrpcQueryBinder.bind(request, ctx)
        ctx.assign(canonical)

        /* Plan query and create execution plan. */
        ctx.plan(this.planner)

        /* Generate operator tree. */
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for handling INSERT queries.
     */
    override suspend fun insert(request: CottontailGrpc.InsertMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        /* Bind query and create logical plan. */
        val canonical = GrpcQueryBinder.bind(request, ctx)
        ctx.assign(canonical)

        /* Implement physical plan. */
        ctx.implement()

        /* Generate operator tree. */
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for handling INSERT BATCH queries.
     */
    override suspend fun insertBatch(request: CottontailGrpc.BatchInsertMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        /* Bind query and create logical plan. */
        val canonical = GrpcQueryBinder.bind(request, ctx)
        ctx.assign(canonical)

        /* Implement physical plan. */
        ctx.implement()

        /* Generate operator tree. */
        ctx.toOperatorTree()
    }.single()
}
