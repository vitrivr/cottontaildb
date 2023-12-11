package org.vitrivr.cottontail.server.grpc.services

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.single
import org.vitrivr.cottontail.client.language.extensions.parse
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.services.AutoRebuilderService
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.operators.physical.definition.*
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.InMemorySortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnDefinition.Compression.NONE
import org.vitrivr.cottontail.grpc.CottontailGrpc.ColumnDefinition.Compression.SNAPPY
import org.vitrivr.cottontail.grpc.DDLGrpcKt
import org.vitrivr.cottontail.storage.serializers.tablets.Compression
import kotlin.time.ExperimentalTime

/**
 * This is a gRPC service endpoint that handles DDL (= Data Definition Language) request for Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.7.1
 */
@ExperimentalTime
class DDLService(override val catalogue: DefaultCatalogue, val autoRebuilderService: AutoRebuilderService) : DDLGrpcKt.DDLCoroutineImplBase(), TransactionalGrpcService {

    /**
     * gRPC endpoint for creating a new [Schema]
     */
    override suspend fun createSchema(request: CottontailGrpc.CreateSchemaMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val schemaName = request.schema.parse()
        ctx.register(CreateSchemaPhysicalOperatorNode(this.catalogue.newTx(ctx), schemaName, request.mayExist, ctx))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for dropping a [Schema]
     */
    override suspend fun dropSchema(request: CottontailGrpc.DropSchemaMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val schemaName = request.schema.parse()
        ctx.register(DropSchemaPhysicalOperatorNode(this.catalogue.newTx(ctx), schemaName, ctx))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint listing the available [Schema]s.
     */
    override fun listSchemas(request: CottontailGrpc.ListSchemaMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata, true) { ctx ->
        val list = ListSchemaPhysicalOperatorNode(this.catalogue.newTx(ctx), ctx)
        ctx.register(InMemorySortPhysicalOperatorNode(list, listOf(list.columns.first()to SortOrder.ASCENDING)))
        ctx.toOperatorTree()
    }

    /**
     * gRPC endpoint for creating a new [Entity]
     */
    override suspend fun createEntity(request: CottontailGrpc.CreateEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val entityName = request.entity.parse()
        try {
            val columns = request.columnsList.map {
                val type = Types.forName(it.type.name, it.length)
                val name = entityName.column(it.name.name) /* To make sure that columns belongs to entity. */
                val compression = when (it.compression) {
                    NONE -> Compression.NONE
                    SNAPPY -> Compression.SNAPPY
                    else -> Compression.LZ4
                }
                name to ColumnMetadata(type, compression, it.nullable, it.primary, it.autoIncrement)
            }
            ctx.register(CreateEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName, request.mayExist, columns, ctx))
            ctx.toOperatorTree()
        }  catch (e: IllegalArgumentException) {
            throw DatabaseException.ValidationException("Invalid entity definition: ${e.message}")
        }
    }.single()

    /**
     * gRPC endpoint for dropping a specific [Entity].
     */
    override suspend fun dropEntity(request: CottontailGrpc.DropEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val entityName = request.entity.parse()
        ctx.register(DropEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName, ctx))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for truncating a specific [Entity].
     */
    override suspend fun truncateEntity(request: CottontailGrpc.TruncateEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val entityName = request.entity.parse()
        ctx.register(TruncateEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName, ctx))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for optimizing a particular [Entity].
     */
    override suspend fun analyzeEntity(request: CottontailGrpc.AnalyzeEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val entityName = request.entity.parse()
        ctx.register(AnalyseEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName, ctx))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint listing the available [Entity]s for the provided [Schema].
     */
    override fun listEntities(request: CottontailGrpc.ListEntityMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata, true) { ctx ->
        val schemaName = if (request.hasSchema()) { request.schema.parse() } else { null }
        val list = ListEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), schemaName, ctx)
        ctx.register(InMemorySortPhysicalOperatorNode(list, listOf(list.columns.first() to SortOrder.ASCENDING)))
        ctx.toOperatorTree()
    }

    /**
     * gRPC endpoint for requesting details about a specific [Entity].
     */
    override suspend fun entityDetails(request: CottontailGrpc.EntityDetailsMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, true) { ctx ->
        val entityName = request.entity.parse()
        ctx.register(AboutEntityPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName, ctx))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for requesting statistics about a specific [Entity].
     */
    override suspend fun entityStatistics(request: CottontailGrpc.EntityDetailsMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, true) { ctx ->
        val entityName = request.entity.parse()
        ctx.register(EntityStatisticsPhysicalOperatorNode(this.catalogue.newTx(ctx), entityName, ctx))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for creating a particular [Index]
     */
    override suspend fun createIndex(request: CottontailGrpc.CreateIndexMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val entityName = request.entity.parse()
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
        ctx.register(CreateIndexPhysicalOperatorNode(this.catalogue.newTx(ctx), indexName, indexType, columns, params, ctx))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for dropping a particular [Index]
     */
    override suspend fun dropIndex(request: CottontailGrpc.DropIndexMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val indexName = request.index.parse()
        ctx.register(DropIndexPhysicalOperatorNode(this.catalogue.newTx(ctx), indexName, ctx))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for requesting details about a specific [Index].
     */
    override suspend fun indexDetails(request: CottontailGrpc.IndexDetailsMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, true) { ctx ->
        val indexName = request.index.parse()
        ctx.register(AboutIndexPhysicalOperatorNode(this.catalogue.newTx(ctx), indexName, ctx))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for rebuilding a particular [Index]
     */
    override suspend fun rebuildIndex(request: CottontailGrpc.RebuildIndexMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata, false) { ctx ->
        val indexName = request.index.parse()
        if (request.async) {
            ctx.register(RebuildIndexPhysicalOperatorNode(this.catalogue.newTx(ctx), indexName, this.autoRebuilderService, ctx))
        } else {
            ctx.register(RebuildIndexPhysicalOperatorNode(this.catalogue.newTx(ctx), indexName, null, ctx))
        }
        ctx.toOperatorTree()
    }.single()
}
