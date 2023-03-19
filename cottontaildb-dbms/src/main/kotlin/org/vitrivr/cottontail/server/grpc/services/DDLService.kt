package org.vitrivr.cottontail.server.grpc.services

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.single
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.services.AutoRebuilderService
import org.vitrivr.cottontail.dbms.execution.services.StatisticsManagerService
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.queries.operators.physical.definition.*
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpcKt
import org.vitrivr.cottontail.utilities.extensions.fqn
import kotlin.time.ExperimentalTime

/**
 * This is a gRPC service endpoint that handles DDL (= Data Definition Language) request for Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.6.0
 */
@ExperimentalTime
class DDLService(override val catalogue: DefaultCatalogue, override val manager: TransactionManager, val autoRebuilderService: AutoRebuilderService, val statisticsManagerService: StatisticsManagerService) : DDLGrpcKt.DDLCoroutineImplBase(), TransactionalGrpcService {
    /**
     * gRPC endpoint for creating a new [Schema]
     */
    override suspend fun createSchema(request: CottontailGrpc.CreateSchemaMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val schemaName = request.schema.fqn()
        ctx.assign(CreateSchemaPhysicalOperatorNode(this.catalogue.newTx(ctx), schemaName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for dropping a [Schema]
     */
    override suspend fun dropSchema(request: CottontailGrpc.DropSchemaMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val schemaName = request.schema.fqn()
        ctx.assign(DropSchemaPhysicalOperatorNode(this.catalogue.newTx(ctx), schemaName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint listing the available [Schema]s.
     */
    override fun listSchemas(request: CottontailGrpc.ListSchemaMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata, true) { ctx ->
        ctx.assign(SortPhysicalOperatorNode(ListSchemaPhysicalOperatorNode(this.catalogue.newTx(ctx)), listOf(Pair(ColumnSets.DDL_LIST_COLUMNS[0], SortOrder.ASCENDING))))
        ctx.toOperatorTree()
    }

    /**
     * gRPC endpoint for creating a new [Entity]
     */
    override suspend fun createEntity(request: CottontailGrpc.CreateEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val entityName = request.definition.entity.fqn()
        val columns = request.definition.columnsList.map {
            val type = Types.forName(it.type.name, it.length)
            val name = entityName.column(it.name.name) /* To make sure that columns belongs to entity. */
            ColumnDef(name, type, it.nullable)
        }.toTypedArray()
        ctx.assign(CreateEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName, columns))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for dropping a specific [Entity].
     */
    override suspend fun dropEntity(request: CottontailGrpc.DropEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val entityName = request.entity.fqn()
        ctx.assign(DropEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for truncating a specific [Entity].
     */
    override suspend fun truncateEntity(request: CottontailGrpc.TruncateEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val entityName = request.entity.fqn()
        ctx.assign(TruncateEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for optimizing a particular [Entity].
     */
    override suspend fun analyzeEntity(request: CottontailGrpc.AnalyzeEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val entityName = request.entity.fqn()
        if (request.async) {
            ctx.assign(AnalyseEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName, this.autoAnalyzerService))
        } else {
            ctx.assign(AnalyseEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName))
        }
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint listing the available [Entity]s for the provided [Schema].
     */
    override fun listEntities(request: CottontailGrpc.ListEntityMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata, true) { ctx ->
        val schemaName = if (request.hasSchema()) { request.schema.fqn() } else { null }
        ctx.assign(SortPhysicalOperatorNode(ListEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), schemaName), listOf(Pair(ColumnSets.DDL_LIST_COLUMNS[0], SortOrder.ASCENDING))))
        ctx.toOperatorTree()
    }

    /**
     * gRPC endpoint for requesting details about a specific [Entity].
     */
    override suspend fun entityDetails(request: CottontailGrpc.EntityDetailsMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, true) { ctx ->
        val entityName = request.entity.fqn()
        ctx.assign(AboutEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for requesting statistics about a specific [Entity].
     */
    override suspend fun entityStatistics(request: CottontailGrpc.EntityDetailsMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, true) { ctx ->
        val entityName = request.entity.fqn()
        ctx.assign(EntityStatisticsPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for creating a particular [Index]
     */
    override suspend fun createIndex(request: CottontailGrpc.CreateIndexMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val entityName = request.entity.fqn()
        val columns = request.columnsList.map {
            if (it.contains('.')) throw QueryException.QuerySyntaxException("Column name '$it' must not contain dots for CREATE INDEX command.")
            entityName.column(it)
        }
        val indexType = IndexType.valueOf(request.type.toString())
        val params = request.paramsMap
        val indexName = if (request.indexName != null && request.indexName.isNotEmpty()) {
            if (request.indexName.contains('.')) throw QueryException.QuerySyntaxException("Index name '${request.indexName}' must not contain dots for CREATE INDEX command.")
            entityName.index(request.indexName)
        } else {
            entityName.index("idx_${request.columnsList.joinToString("-")}_${indexType.name.lowercase()}")
        }
        ctx.assign(CreateIndexPhysicalOperatorNode(this.catalogue.newTx(ctx), indexName, indexType, columns, params))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for dropping a particular [Index]
     */
    override suspend fun dropIndex(request: CottontailGrpc.DropIndexMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val indexName = request.index.fqn()
        ctx.assign(DropIndexPhysicalOperatorNode(this.catalogue.newTx(ctx), indexName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for requesting details about a specific [Index].
     */
    override suspend fun indexDetails(request: CottontailGrpc.IndexDetailsMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, true) { ctx ->
        val indexName = request.index.fqn()
        ctx.assign(AboutIndexPhysicalOperatorNode(this.catalogue.newTx(ctx), indexName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for rebuilding a particular [Index]
     */
    override suspend fun rebuildIndex(request: CottontailGrpc.RebuildIndexMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val indexName = request.index.fqn()
        if (request.async) {
            ctx.assign(RebuildIndexPhysicalOperatorNode(this.catalogue.newTx(ctx), indexName, this.autoRebuilderService))
        } else {
            ctx.assign(RebuildIndexPhysicalOperatorNode(this.catalogue.newTx(ctx), indexName))
        }
        ctx.toOperatorTree()
    }.single()
}
