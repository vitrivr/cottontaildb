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
 * @version 2.2.0
 */
@ExperimentalTime
class DDLService(override val catalogue: DefaultCatalogue, override val manager: TransactionManager) : DDLGrpcKt.DDLCoroutineImplBase(), TransactionalGrpcService {

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.database.schema.Schema]
     */
    override suspend fun createSchema(request: CottontailGrpc.CreateSchemaMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) {
        val schemaName = request.schema.fqn()
        CreateSchemaOperator(this.catalogue, schemaName)
    }.single()

    /**
     * gRPC endpoint for dropping a [org.vitrivr.cottontail.database.schema.Schema]
     */
    override suspend fun dropSchema(request: CottontailGrpc.DropSchemaMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) {
        val schemaName = request.schema.fqn()
        DropSchemaOperator(this.catalogue, schemaName)
    }.single()
    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.database.schema.DefaultSchema]s.
     */
    override fun listSchemas(request: CottontailGrpc.ListSchemaMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata) {
        HeapSortOperator(ListSchemaOperator(this.catalogue), listOf(Pair(ListSchemaOperator.COLUMNS[0], SortOrder.ASCENDING)), 100)
    }

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.database.entity.Entity]
     */
    override suspend fun createEntity(request: CottontailGrpc.CreateEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) {
        val entityName = request.definition.entity.fqn()
        val columns = request.definition.columnsList.map {
            val type = Type.forName(it.type.name, it.length)
            val name = entityName.column(it.name.name) /* To make sure that columns belongs to entity. */
            ColumnDef(name, type, it.nullable) to ColumnEngine.valueOf(it.engine.toString())
        }.toTypedArray()
        CreateEntityOperator(this.catalogue, entityName, columns)
    }.single()

    /**
     * gRPC endpoint for dropping a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override suspend fun dropEntity(request: CottontailGrpc.DropEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) {
        val entityName = request.entity.fqn()
        DropEntityOperator(this.catalogue, entityName)
    }.single()

    /**
     * gRPC endpoint for truncating a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override suspend fun truncateEntity(request: CottontailGrpc.TruncateEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) {
        val entityName = request.entity.fqn()
        TruncateEntityOperator(this.catalogue, entityName)
    }.single()

    /**
     * gRPC endpoint for optimizing a particular [org.vitrivr.cottontail.database.entity.Entity].
     */
    override suspend fun optimizeEntity(request: CottontailGrpc.OptimizeEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) {
        val entityName = request.entity.fqn()
        OptimizeEntityOperator(this.catalogue, entityName)
    }.single()

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.database.entity.Entity]s for the provided [org.vitrivr.cottontail.database.schema.Schema].
     */
    override fun listEntities(request: CottontailGrpc.ListEntityMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata) {
        val schemaName = if (request.hasSchema()) {
            request.schema.fqn()
        } else {
            null
        }
        HeapSortOperator(ListEntityOperator(this.catalogue, schemaName), listOf(Pair(ListSchemaOperator.COLUMNS[0], SortOrder.ASCENDING)), 100)
    }

    /**
     * gRPC endpoint for requesting details about a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override suspend fun entityDetails(request: CottontailGrpc.EntityDetailsMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) {
        val entityName = request.entity.fqn()
        EntityDetailsOperator(this.catalogue, entityName)
    }.single()

    /**
     * gRPC endpoint for creating a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override suspend fun createIndex(request: CottontailGrpc.CreateIndexMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) {
        val indexName = request.definition.name.fqn()
        val columns = request.definition.columnsList.map {
            indexName.entity().column(it.name)
        }
        val indexType = IndexType.valueOf(request.definition.type.toString())
        val params = request.definition.paramsMap
        CreateIndexOperator(this.catalogue, indexName, indexType, columns, params, request.rebuild)
    }.single()

    /**
     * gRPC endpoint for dropping a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override suspend fun dropIndex(request: CottontailGrpc.DropIndexMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) {
        val indexName = request.index.fqn()
        DropIndexOperator(this.catalogue, indexName)
    }.single()

    /**
     * gRPC endpoint for rebuilding a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override suspend fun rebuildIndex(request: CottontailGrpc.RebuildIndexMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) {
        val indexName = request.index.fqn()
        RebuildIndexOperator(this.catalogue, indexName)
    }.single()
}
