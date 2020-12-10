package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.column.ColumnType
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.server.grpc.helper.fqn
import org.vitrivr.cottontail.server.grpc.helper.proto

/**
 * This is a gRPC service endpoint that handles DDL (=Data Definition Language) request for Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class CottonDDLService(val catalogue: Catalogue) : CottonDDLGrpc.CottonDDLImplBase() {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(CottonDDLService::class.java)
    }

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.database.schema.Schema]s.
     */
    override fun listSchemas(request: CottontailGrpc.Empty, responseObserver: StreamObserver<CottontailGrpc.Schema>) = try {
        this.catalogue.schemas.forEach {
            responseObserver.onNext(CottontailGrpc.Schema.newBuilder().setName(it.simple).build())
        }
        responseObserver.onCompleted()
    } catch (e: DatabaseException) {
        val message = "Failed to fetch list of schemas because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to fetch list of schemas because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.database.schema.Schema]
     */
    override fun createSchema(request: CottontailGrpc.Schema, responseObserver: StreamObserver<CottontailGrpc.Status>) = try {
        val schemaName = request.fqn()
        LOGGER.info("Creating schema '$schemaName'...")
        this.catalogue.createSchema(schemaName)
        LOGGER.info("Schema '$schemaName' created successfully!")
        responseObserver.onCompleted()
    } catch (e: DatabaseException.EntityAlreadyExistsException) {
        val message = "Failed to create schema '${request.fqn()}': Schema with identical name already exists."
        LOGGER.error(message)
        responseObserver.onError(Status.ALREADY_EXISTS.withDescription(message).asException())
    } catch (e: DatabaseException) {
        val message = "Failed to create schema '${request.fqn()}' because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to create schema '${request.fqn()}' because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }

    /**
     * gRPC endpoint for dropping a [org.vitrivr.cottontail.database.schema.Schema]
     */
    override fun dropSchema(request: CottontailGrpc.Schema, responseObserver: StreamObserver<CottontailGrpc.Status>) = try {
        val schemaName = request.fqn()
        LOGGER.info("Dropping schema '$schemaName'...")
        this.catalogue.dropSchema(schemaName)
        LOGGER.info("Schema '$schemaName' dropped successfully!")
        responseObserver.onCompleted()
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        val message = "Failed to drop schema '${request.fqn()}': Schema does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException) {
        val message = "Failed to drop schema '${request.fqn()}' because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to drop schema '${request.fqn()}' because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }

    /**
     * gRPC endpoint for requesting details about a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override fun entityDetails(request: CottontailGrpc.Entity, responseObserver: StreamObserver<CottontailGrpc.EntityDefinition>) = try {
        val entityName = request.fqn()
        val entity = this.catalogue.schemaForName(entityName.schema()).entityForName(entityName)
        val def = CottontailGrpc.EntityDefinition.newBuilder().setEntity(request)
        for (c in entity.allColumns()) {
            def.addColumns(CottontailGrpc.ColumnDefinition.newBuilder()
                    .setEngine(CottontailGrpc.Engine.MAPDB)
                    .setName(c.name.simple)
                    .setNullable(c.nullable)
                    .setLength(c.logicalSize)
                    .setType(CottontailGrpc.Type.valueOf(c.type.name))
            )
        }
        responseObserver.onCompleted()
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        val message = "Failed to fetch entity information '${request.fqn()}': Schema does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        val message = "Failed to fetch entity information '${request.fqn()}': Entity does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException) {
        val message = "Failed to fetch entity information '${request.fqn()}' because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to fetch entity information '${request.fqn()}' because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }

    /**
     * gRPC endpoint for creating a new [org.vitrivr.cottontail.database.entity.Entity]
     */
    override fun createEntity(request: CottontailGrpc.EntityDefinition, responseObserver: StreamObserver<CottontailGrpc.Status>) = try {
        val entityName = request.entity.fqn()
        LOGGER.info("Creating entity '$entityName'...")
        val schema = this.catalogue.schemaForName(entityName.schema())
        val columns = request.columnsList.map {
            val type = ColumnType.forName(it.type.name)
            val name = entityName.column(it.name)
            ColumnDef(name, type, it.length, it.nullable)
        }
        schema.createEntity(entityName, *columns.toTypedArray())
        LOGGER.info("Entity '$entityName' created successfully!")
        responseObserver.onCompleted()
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        val message = "Failed to create entity '${request.entity.fqn()}': Schema does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.EntityAlreadyExistsException) {
        val message = "Failed to create entity '${request.entity.fqn()}': Entity with identical name already exists."
        LOGGER.error(message)
        responseObserver.onError(Status.ALREADY_EXISTS.withDescription(message).asException())
    } catch (e: DatabaseException) {
        val message = "Failed to create entity '${request.entity.fqn()}' because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to create entity '${request.entity.fqn()}' because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }

    /**
     * gRPC endpoint for dropping a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override fun dropEntity(request: CottontailGrpc.Entity, responseObserver: StreamObserver<CottontailGrpc.Status>) = try {
        val entityName = request.fqn()
        LOGGER.info("Dropping entity '$entityName'...")
        this.catalogue.schemaForName(entityName.schema()).dropEntity(entityName)
        responseObserver.onNext(CottontailGrpc.Status.newBuilder().setSuccess(true).setTimestamp(System.currentTimeMillis()).build())
        responseObserver.onCompleted()
        LOGGER.info("Entity '$entityName' dropped successfully!")
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        val message = "Failed to drop entity '${request.fqn()}': Schema does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        val message = "Failed to drop entity '${request.fqn()}': Entity does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException) {
        val message = "Failed to drop entity '${request.fqn()}' because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to drop entity '${request.fqn()}' because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }

    /**
     * gRPC endpoint for truncating a specific [org.vitrivr.cottontail.database.entity.Entity].
     */
    override fun truncate(request: CottontailGrpc.Entity, responseObserver: StreamObserver<CottontailGrpc.Status>) = try {
        val entityName = request.fqn()
        LOGGER.info("Truncating entity '$entityName'...", entityName)

        /* Drop and re-create entity. */
        val schema = this.catalogue.schemaForName(entityName.schema())
        val columns = schema.entityForName(entityName).allColumns().toTypedArray()
        schema.dropEntity(entityName)
        schema.createEntity(entityName, *columns)

        /* Notify caller about success. */
        responseObserver.onCompleted()
        LOGGER.info("Entity '$entityName' truncated successfully!", request)
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        val message = "Failed to truncate entity '${request.fqn()}': Schema does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        val message = "Failed to truncate entity '${request.fqn()}': Entity does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException) {
        val message = "Failed to truncate entity '${request.fqn()}' because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to truncate entity '${request.fqn()}' because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }

    /**
     * gRPC endpoint listing the available [org.vitrivr.cottontail.database.entity.Entity]s for the provided [org.vitrivr.cottontail.database.schema.Schema].
     */
    override fun listEntities(request: CottontailGrpc.Schema, responseObserver: StreamObserver<CottontailGrpc.Entity>) = try {
        val schemaName = request.fqn()
        val builder = CottontailGrpc.Entity.newBuilder()
        this.catalogue.schemaForName(schemaName).entities.forEach {
            responseObserver.onNext(builder.setName(it.simple).setSchema(request).build())
        }
        responseObserver.onCompleted()
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        val message = "Failed to list entities for schema '${request.fqn()}': Schema does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException) {
        val message = "Failed to list entities for schema '${request.fqn()}' because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to list entities for schema '${request.fqn()}' because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }

    /**
     * gRPC endpoint for creating a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override fun createIndex(request: CottontailGrpc.IndexDefinition, responseObserver: StreamObserver<CottontailGrpc.Status>) = try {
        val indexName = request.index.fqn()
        LOGGER.info("Creating index '$indexName'...")
        val entity = this.catalogue.schemaForName(indexName.schema()).entityForName(indexName.entity())
        val columns = request.columnsList.map {
            val columnName = indexName.entity().column(it)
            entity.columnForName(columnName)
                    ?: throw DatabaseException.ColumnDoesNotExistException(columnName)
        }.toTypedArray()

        /* Creates and updates the index. */
        entity.createIndex(indexName, IndexType.valueOf(request.index.type.toString()), columns, request.paramsMap)

        /* Notify caller of success. */
        responseObserver.onCompleted()
        LOGGER.info("Index '$indexName' created successfully!", request)
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        val message = "Failed to create index '${request.index.fqn()}': Schema does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        val message = "Failed to create index '${request.index.fqn()}': Entity does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.ColumnDoesNotExistException) {
        val message = "Failed to create index '${request.index.fqn()}': Column does not exist."
        LOGGER.error(message, e)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.IndexAlreadyExistsException) {
        val message = "Failed to create index '${request.index.fqn()}': Index with identical name does already exist."
        LOGGER.error(message, e)
        responseObserver.onError(Status.ALREADY_EXISTS.withDescription(message).asException())
    } catch (e: DatabaseException) {
        val message = "Failed to create index '${request.index.fqn()}' because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to create index '${request.index.fqn()}' because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }

    /**
     * gRPC endpoint for dropping a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override fun dropIndex(request: CottontailGrpc.Index, responseObserver: StreamObserver<CottontailGrpc.Status>) = try {
        val indexName = request.fqn()
        LOGGER.info("Dropping index '$indexName'...")
        this.catalogue.schemaForName(indexName.schema()).entityForName(indexName.entity()).dropIndex(indexName)

        /* Notify caller of success. */
        responseObserver.onCompleted()
        LOGGER.info("Index '$indexName' dropped successfully!")
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        val message = "Failed to drop index '${request.fqn()}': Schema does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        val message = "Failed to drop index '${request.fqn()}': Entity does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.IndexDoesNotExistException) {
        val message = "Failed to drop index '${request.fqn()}': Index does not exist."
        LOGGER.error(message, e)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException) {
        val message = "Failed to drop index '${request.fqn()}' because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to drop index '${request.fqn()}' because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }

    /**
     * gRPC endpoint for rebuilding a particular [org.vitrivr.cottontail.database.index.Index]
     */
    override fun rebuildIndex(request: CottontailGrpc.Index, responseObserver: StreamObserver<CottontailGrpc.Status>) = try {
        val indexName = request.fqn()
        LOGGER.info("Rebuilding index '$indexName'...")

        /* Update index. */
        this.catalogue.schemaForName(indexName.schema()).entityForName(indexName.entity()).updateIndex(indexName)

        /* Notify caller of success. */
        responseObserver.onCompleted()
        LOGGER.info("Index '$indexName' rebuilt successfully!")
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        val message = "Failed to rebuild index '${request.fqn()}': Schema does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        val message = "Failed to rebuild index '${request.fqn()}': Entity does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.IndexDoesNotExistException) {
        val message = "Failed to rebuild index '${request.fqn()}': Index does not exist."
        LOGGER.error(message, e)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException) {
        val message = "Failed to rebuild index '${request.fqn()}' because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to rebuild index '${request.fqn()}' because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }

    /**
     * gRPC endpoint for listing available [org.vitrivr.cottontail.database.index.Index] for a given [org.vitrivr.cottontail.database.entity.Entity]
     */
    override fun listIndexes(request: CottontailGrpc.Entity, responseObserver: StreamObserver<CottontailGrpc.Index>) = try {
        /* Get entity and stream available index structures. */
        val entityName = request.fqn()
        val entity = this.catalogue.schemaForName(entityName.schema()).entityForName(entityName)
        entity.allIndexes().forEach {
            val index = CottontailGrpc.Index.newBuilder()
                    .setName(it.name.simple)
                    .setType(CottontailGrpc.IndexType.valueOf(it.type.name))
                    .setEntity(entityName.proto())

            it.columns.forEach { c ->
                index.addColumns(CottontailGrpc.ColumnDefinition.newBuilder()
                        .setEngine(CottontailGrpc.Engine.MAPDB)
                        .setName(c.name.simple)
                        .setNullable(c.nullable)
                        .setLength(c.logicalSize)
                        .setType(CottontailGrpc.Type.valueOf(c.type.name))
                )
            }
            responseObserver.onNext(index.build())
        }

        /* Notify caller of success. */
        responseObserver.onCompleted()
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        val message = "Failed to list indexes for entity '${request.fqn()}': Schema does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        val message = "Failed to list indexes for entity '${request.fqn()}': Entity does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException) {
        val message = "Failed to list indexes for entity '${request.fqn()}' because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to list indexes for entity '${request.fqn()}' because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }

    /**
     * gRPC endpoint for optimizing a particular entity. Currently just rebuilds all the indexes.
     */
    override fun optimize(request: CottontailGrpc.Entity, responseObserver: StreamObserver<CottontailGrpc.Status>) = try {
        val entityName = request.fqn()
        LOGGER.info("Optimizing entity '$entityName'...")

        /* Update indexes. */
        this.catalogue.schemaForName(entityName.schema()).entityForName(entityName).updateAllIndexes()

        /* Notify caller about success. */
        LOGGER.info("Entity '$entityName' optimized successfully!")
        responseObserver.onCompleted()
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        val message = "Failed to optimize entity '${request.fqn()}': Schema does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        val message = "Failed to optimize entity '${request.fqn()}': Entity does not exist."
        LOGGER.info(message)
        responseObserver.onError(Status.NOT_FOUND.withDescription(message).asException())
    } catch (e: DatabaseException) {
        val message = "Failed to truncate entity '${request.fqn()}' because of a database error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.DATA_LOSS.withDescription(message).asException())
    } catch (e: Throwable) {
        val message = "Failed to truncate entity '${request.fqn()}' because of an unknown error."
        LOGGER.error(message, e)
        responseObserver.onError(Status.UNKNOWN.withDescription(message).withCause(e).asException())
    }
}
