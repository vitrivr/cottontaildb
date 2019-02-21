package ch.unibas.dmi.dbis.cottontail.server.grpc.services

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.database.column.*
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.execution.ExecutionPlan
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDMLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.StandaloneRecord
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import ch.unibas.dmi.dbis.cottontail.server.grpc.helper.*
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory

internal class CottonDMLService (val catalogue: Catalogue): CottonDMLGrpc.CottonDMLImplBase() {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(CottonDMLService::class.java)
    }

    /**
     * GRPC endpoint for inserting data in a batch mode. All the data provided with the [CottontailGrpc.InsertMessage] will be inserted
     * in a single transaction. I.e. either the insert succeeds or fails completely.
     */
    override fun insert(request: CottontailGrpc.InsertMessage, responseObserver: StreamObserver<CottontailGrpc.InsertStatus>) = try {
        val schema = this.catalogue.getSchema(request.entity.schema.name)
        val entity = schema.getEntity(request.entity.name)
        entity.Tx(false).begin { tx ->
            request.tupleList.map { it.dataMap }.forEach { entry ->
                val columns = mutableListOf<ColumnDef<*>>()
                val values = mutableListOf<Any?>()
                entry.map {
                    val col = entity.columnForName(it.key) ?: throw DatabaseException.ColumnNotExistException(it.key, entity.name)
                    columns.add(col)
                    values.add(castToColumn(it.value, col))
                }
                tx.insert(StandaloneRecord(columns = columns.toTypedArray(), init = values.toTypedArray()))
            }
            LOGGER.trace("Successfully persisted ${request.tupleList.size} tuples to '${request.entity.fqn()}'.")
            true
        }
        responseObserver.onNext(CottontailGrpc.InsertStatus.newBuilder().setSuccess(true).setTimestamp(System.currentTimeMillis()).build())
        responseObserver.onCompleted()
    } catch (e: DatabaseException.SchemaDoesNotExistException) {
        responseObserver.onError(Status.NOT_FOUND.withDescription("Insert failed because schema '${request.entity.schema.name} does not exist!").asException())
    } catch (e: DatabaseException.EntityDoesNotExistException) {
        responseObserver.onError(Status.NOT_FOUND.withDescription("Insert failed because entity '${request.entity.name} does not exist!").asException())
    } catch (e: DatabaseException.ValidationException) {
        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Insert failed because data validation failed: ${e.message}").asException())
    }  catch (e: DatabaseException.ValidationException) {
        responseObserver.onError(Status.UNKNOWN.withDescription("Insert failed because of a database error: ${e.message}").asException())
    }

    /**
     * GRPC endpoint for inserting data in a streaming mode. All the data provided with the [CottontailGrpc.InsertMessage] will be inserted
     * in a single transaction. i.e. either the insert succeeds or fails completely.
     */
    override fun insertStream(responseObserver: StreamObserver<CottontailGrpc.InsertStatus>): StreamObserver<CottontailGrpc.InsertMessage> = object:StreamObserver<CottontailGrpc.InsertMessage>{
        override fun onNext(request: CottontailGrpc.InsertMessage) = try {
            val schema = this@CottonDMLService.catalogue.getSchema(request.entity.schema.name)
            val entity = schema.getEntity(request.entity.name)
            entity.Tx(false).begin { tx ->
                request.tupleList.map { it.dataMap }.forEach { entry ->
                    val columns = mutableListOf<ColumnDef<*>>()
                    val values = mutableListOf<Any?>()
                    entry.map {
                        val col = entity.columnForName(it.key) ?: throw DatabaseException.ColumnNotExistException(it.key, entity.name)
                        columns.add(col)
                        values.add(castToColumn(it.value, col))
                    }
                    tx.insert(StandaloneRecord(columns = columns.toTypedArray(), init = values.toTypedArray()))
                    LOGGER.trace("Successfully persisted ${request.tupleList.size} tuples to '${request.entity.fqn()}'.")
                }
                true
            }
            responseObserver.onNext(CottontailGrpc.InsertStatus.newBuilder().setSuccess(true).setTimestamp(System.currentTimeMillis()).build())
            responseObserver.onCompleted()
        } catch (e: DatabaseException.SchemaDoesNotExistException) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Insert failed because schema '${request.entity.schema.name} does not exist!").asException())
        } catch (e: DatabaseException.EntityDoesNotExistException) {
            responseObserver.onError(Status.NOT_FOUND.withDescription("Insert failed because entity '${request.entity.name} does not exist!").asException())
        } catch (e: DatabaseException.ValidationException) {
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Insert failed because data validation failed: ${e.message}").asException())
        }  catch (e: DatabaseException.ValidationException) {
            responseObserver.onError(Status.UNKNOWN.withDescription("Insert failed because of a database error: ${e.message}").asException())
        }

        override fun onError(t: Throwable?) = responseObserver.onError(Status.UNKNOWN.withCause(t).asException())

        override fun onCompleted() = responseObserver.onCompleted()
    }

    /**
     * Casts the provided [CottontailGrpc.Data] to a data type supported by the provided [ColumnDef]
     *
     * @param value The [CottontailGrpc.Data] to cast.
     * @param col The [ColumnDef] of the column, the data should be stored in.
     * @return The converted value.
     */
    private fun castToColumn(value: CottontailGrpc.Data, col: ColumnDef<*>) : Any? = if (
            value.dataCase == CottontailGrpc.Data.DataCase.DATA_NOT_SET || value.dataCase == null
    ) {
        null
    } else {
        when (col.type) {
            is BooleanColumnType -> value.toBooleanValue()
            is ByteColumnType -> value.toByteValue()
            is ShortColumnType -> value.toShortValue()
            is IntColumnType -> value.toIntValue()
            is LongColumnType -> value.toLongValue()
            is FloatColumnType -> value.toFloatValue()
            is DoubleColumnType -> value.toDoubleValue()
            is StringColumnType -> value.toStringValue()
            is IntArrayColumnType -> value.toIntVectorValue()
            is LongArrayColumnType -> value.toLongVectorValue()
            is FloatArrayColumnType -> value.toFloatVectorValue()
            is DoubleArrayColumnType -> value.toDoubleVectorValue()
        }
    }
}