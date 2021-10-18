package org.vitrivr.cottontail.server.grpc.services

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.single
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnEngine
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.binding.extensions.fqn
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionManager
import org.vitrivr.cottontail.execution.operators.definition.*
import org.vitrivr.cottontail.execution.operators.sort.HeapSortOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpcKt
import org.vitrivr.cottontail.model.basics.Type
import kotlin.time.ExperimentalTime

/**
 * This is a gRPC service endpoint that handles DDL (= Data Definition Language) request for Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
@ExperimentalTime
class DDLService(override val catalogue: DefaultCatalogue, override val manager: TransactionManager) : DDLGrpcKt.DDLCoroutineImplBase(), TransactionalGrpcService {

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.database.schema.Schema]
     */
    override suspend fun createSchema(request: CottontailGrpc.CreateSchemaMessage): CottontailGrpc.QueryResponseMessage {
        val ctx = this.queryContext(request.metadata)
        val schemaName = request.schema.fqn()
        val op = CreateSchemaOperator(this.catalogue, schemaName)
        return executeAndMaterialize(ctx, op).single()
    }

    /**
     * gRPC endpoint for dropping a [org.vitrivr.cottontail.database.schema.Schema]
     */
    override suspend fun dropSchema(request: CottontailGrpc.DropSchemaMessage): CottontailGrpc.QueryResponseMessage {
        val ctx = this.queryContext(request.metadata)
        val schemaName = request.schema.fqn()
        val op = DropSchemaOperator(this.catalogue, schemaName)
        return executeAndMaterialize(ctx, op).single()
    }

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.database.schema.DefaultSchema]s.
     */
    override fun listSchemas(request: CottontailGrpc.ListSchemaMessage): Flow<CottontailGrpc.QueryResponseMessage> {
        val ctx = this.queryContext(request.metadata)
        val op = HeapSortOperator(ListSchemaOperator(this.catalogue), listOf(Pair(ListSchemaOperator.COLUMNS[0], SortOrder.ASCENDING)), 100)
        return executeAndMaterialize(ctx, op)
    }

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.database.entity.Entity]
     */
    override suspend fun createEntity(request: CottontailGrpc.CreateEntityMessage): CottontailGrpc.QueryResponseMessage {
        val ctx = this.queryContext(request.metadata)
        val entityName = request.definition.entity.fqn()
        val columns = request.definition.columnsList.map {
            val type = Type.forName(it.type.name, it.length)
            val name = it.name.fqn()
            ColumnDef(name, type, it.nullable) to ColumnEngine.valueOf(it.engine.toString())
        }.toTypedArray()
        val op = CreateEntityOperator(this.catalogue, entityName, columns)
        return executeAndMaterialize(ctx, op).single()
    }

    /**
     * gRPC endpoint for dropping a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override suspend fun dropEntity(request: CottontailGrpc.DropEntityMessage): CottontailGrpc.QueryResponseMessage {
        val ctx = this.queryContext(request.metadata)
        val entityName = request.entity.fqn()
        val op = DropEntityOperator(this.catalogue, entityName)
        return executeAndMaterialize(ctx, op).single()
    }

    /**
     * gRPC endpoint for truncating a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override suspend fun truncateEntity(request: CottontailGrpc.TruncateEntityMessage): CottontailGrpc.QueryResponseMessage {
        val ctx = this.queryContext(request.metadata)
        val entityName = request.entity.fqn()
        val op = TruncateEntityOperator(this.catalogue, entityName)
        return executeAndMaterialize(ctx, op).single()
    }

    /**
     * gRPC endpoint for optimizing a particular [org.vitrivr.cottontail.database.entity.Entity].
     */
    override suspend fun optimizeEntity(request: CottontailGrpc.OptimizeEntityMessage): CottontailGrpc.QueryResponseMessage {
        val ctx = this.queryContext(request.metadata)
        val entityName = request.entity.fqn()
        val op = OptimizeEntityOperator(this.catalogue, entityName)
        return executeAndMaterialize(ctx, op).single()
    }

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.database.entity.Entity]s for the provided [org.vitrivr.cottontail.database.schema.Schema].
     */
    override fun listEntities(request: CottontailGrpc.ListEntityMessage): Flow<CottontailGrpc.QueryResponseMessage> {
        val ctx = this.queryContext(request.metadata)
        val schemaName = if (request.hasSchema()) {
            request.schema.fqn()
        } else {
            null
        }
        val op = HeapSortOperator(ListEntityOperator(this.catalogue, schemaName), listOf(Pair(ListSchemaOperator.COLUMNS[0], SortOrder.ASCENDING)), 100)
        return executeAndMaterialize(ctx, op)
    }

    /**
     * gRPC endpoint for requesting details about a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override suspend fun entityDetails(request: CottontailGrpc.EntityDetailsMessage): CottontailGrpc.QueryResponseMessage {
        val ctx = this.queryContext(request.metadata)
        val entityName = request.entity.fqn()
        val op = EntityDetailsOperator(this.catalogue, entityName)
        return executeAndMaterialize(ctx, op).single()
    }

    /**
     * gRPC endpoint for creating a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override suspend fun createIndex(request: CottontailGrpc.CreateIndexMessage): CottontailGrpc.QueryResponseMessage {
        val ctx = this.queryContext(request.metadata)
        val indexName = request.definition.name.fqn()
        val columns = request.definition.columnsList.map {
            indexName.entity().column(it.name)
        }
        val indexType = IndexType.valueOf(request.definition.type.toString())
        val params = request.definition.paramsMap
        val op = CreateIndexOperator(this.catalogue, indexName, indexType, columns, params, request.rebuild)
        return executeAndMaterialize(ctx, op).single()
    }

    /**
     * gRPC endpoint for dropping a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override suspend fun dropIndex(request: CottontailGrpc.DropIndexMessage): CottontailGrpc.QueryResponseMessage {
        val ctx = this.queryContext(request.metadata)
        val indexName = request.index.fqn()
        val op = DropIndexOperator(this.catalogue, indexName)
        return executeAndMaterialize(ctx, op).single()
    }

    /**
     * gRPC endpoint for rebuilding a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override suspend fun rebuildIndex(request: CottontailGrpc.RebuildIndexMessage): CottontailGrpc.QueryResponseMessage  {
        val ctx = this.queryContext(request.metadata)
        val indexName = request.index.fqn()
        val op = RebuildIndexOperator(this.catalogue, indexName)
        return executeAndMaterialize(ctx, op).single()
    }
}
