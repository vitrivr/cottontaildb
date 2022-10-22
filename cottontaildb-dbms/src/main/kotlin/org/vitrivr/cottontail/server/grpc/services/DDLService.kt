package org.vitrivr.cottontail.server.grpc.services

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.single
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.queries.operators.physical.definition.*
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.DDLGrpcKt
import org.vitrivr.cottontail.utilities.extensions.fqn
import kotlin.time.ExperimentalTime

/**
 * This is a gRPC service endpoint that handles DDL (= Data Definition Language) request for Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
@ExperimentalTime
class DDLService(override val catalogue: DefaultCatalogue, override val manager: TransactionManager) : DDLGrpcKt.DDLCoroutineImplBase(), TransactionalGrpcService {

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.dbms.schema.Schema]
     */
    override suspend fun createSchema(request: CottontailGrpc.CreateSchemaMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) { ctx ->
        val schemaName = request.schema.fqn()
        ctx.assign(CreateSchemaPhysicalOperatorNode(ctx.txn.getTx(this.catalogue) as CatalogueTx, schemaName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for dropping a [org.vitrivr.cottontail.dbms.schema.Schema]
     */
    override suspend fun dropSchema(request: CottontailGrpc.DropSchemaMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) { ctx ->
        val schemaName = request.schema.fqn()
        ctx.assign(DropSchemaPhysicalOperatorNode(ctx.txn.getTx(this.catalogue) as CatalogueTx, schemaName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.dbms.schema.DefaultSchema]s.
     */
    override fun listSchemas(request: CottontailGrpc.ListSchemaMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata) { ctx ->
        ctx.assign(SortPhysicalOperatorNode(ListSchemaPhysicalOperatorNode(ctx.txn.getTx(this.catalogue) as CatalogueTx), listOf(Pair(ColumnSets.DDL_LIST_COLUMNS[0], SortOrder.ASCENDING))))
        ctx.toOperatorTree()
    }

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.dbms.entity.Entity]
     */
    override suspend fun createEntity(request: CottontailGrpc.CreateEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) { ctx ->
        val entityName = request.definition.entity.fqn()
        val columns = request.definition.columnsList.map {
            val type = Types.forName(it.type.name, it.length)
            val name = entityName.column(it.name.name) /* To make sure that columns belongs to entity. */
            try {
                ColumnDef(name, type, it.nullable, it.primary, it.autoIncrement)
            } catch (e: IllegalArgumentException) {
                throw DatabaseException.ValidationException(e.message ?: "Failed to validate query input.")
            }
        }.toTypedArray()
        ctx.assign(CreateEntityPhysicalOperatorNode(ctx.txn.getTx(this.catalogue) as CatalogueTx, entityName, columns))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for dropping a specific [org.vitrivr.cottontail.dbms.entity.Entity].
     */
    override suspend fun dropEntity(request: CottontailGrpc.DropEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) { ctx ->
        val entityName = request.entity.fqn()
        ctx.assign(DropEntityPhysicalOperatorNode(ctx.txn.getTx(this.catalogue) as CatalogueTx, entityName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for truncating a specific [org.vitrivr.cottontail.dbms.entity.Entity].
     */
    override suspend fun truncateEntity(request: CottontailGrpc.TruncateEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) { ctx ->
        val entityName = request.entity.fqn()
        ctx.assign(TruncateEntityPhysicalOperatorNode(ctx.txn.getTx(this.catalogue) as CatalogueTx, entityName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for optimizing a particular [org.vitrivr.cottontail.dbms.entity.Entity].
     */
    override suspend fun optimizeEntity(request: CottontailGrpc.OptimizeEntityMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) { ctx ->
        val entityName = request.entity.fqn()
        ctx.assign(OptimizeEntityPhysicalOperatorNode(ctx.txn.getTx(this.catalogue) as CatalogueTx, entityName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.dbms.entity.Entity]s for the provided [org.vitrivr.cottontail.dbms.schema.Schema].
     */
    override fun listEntities(request: CottontailGrpc.ListEntityMessage): Flow<CottontailGrpc.QueryResponseMessage> = prepareAndExecute(request.metadata) { ctx ->
        val schemaName = if (request.hasSchema()) { request.schema.fqn() } else { null }
        ctx.assign(SortPhysicalOperatorNode(ListEntityPhysicalOperatorNode(ctx.txn.getTx(this.catalogue) as CatalogueTx, schemaName), listOf(Pair(ColumnSets.DDL_LIST_COLUMNS[0], SortOrder.ASCENDING))))
        ctx.toOperatorTree()
    }

    /**
     * gRPC endpoint for requesting details about a specific [org.vitrivr.cottontail.dbms.entity.Entity].
     */
    override suspend fun entityDetails(request: CottontailGrpc.EntityDetailsMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) { ctx ->
        val entityName = request.entity.fqn()
        ctx.assign(AboutEntityPhysicalOperatorNode(ctx.txn.getTx(this.catalogue) as CatalogueTx, entityName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for creating a particular [org.vitrivr.cottontail.dbms.index.Index]
     */
    override suspend fun createIndex(request: CottontailGrpc.CreateIndexMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) { ctx ->
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
        ctx.assign(CreateIndexPhysicalOperatorNode(ctx.txn.getTx(this.catalogue) as CatalogueTx, indexName, indexType, columns, params, request.rebuild))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for dropping a particular [org.vitrivr.cottontail.dbms.index.Index]
     */
    override suspend fun dropIndex(request: CottontailGrpc.DropIndexMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) { ctx ->
        val indexName = request.index.fqn()
        ctx.assign(DropIndexPhysicalOperatorNode(ctx.txn.getTx(this.catalogue) as CatalogueTx, indexName))
        ctx.toOperatorTree()
    }.single()

    /**
     * gRPC endpoint for rebuilding a particular [org.vitrivr.cottontail.dbms.index.Index]
     */
    override suspend fun rebuildIndex(request: CottontailGrpc.RebuildIndexMessage): CottontailGrpc.QueryResponseMessage = prepareAndExecute(request.metadata) { ctx ->
        val indexName = request.index.fqn()
        ctx.assign(RebuildIndexPhysicalOperatorNode(ctx.txn.getTx(this.catalogue) as CatalogueTx, indexName))
        ctx.toOperatorTree()
    }.single()
}
