package ch.unibas.dmi.dbis.cottontail.server.grpc.services

import ch.unibas.dmi.dbis.cottontail.database.catalogue.Catalogue
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.grpc.CottonDMLGrpc
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc
import ch.unibas.dmi.dbis.cottontail.model.basics.StandaloneRecord
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import io.grpc.Status
import io.grpc.stub.StreamObserver

internal class CottonDMLService (val catalogue: Catalogue): CottonDMLGrpc.CottonDMLImplBase() {
    /**
     * GRPC endpoint for inserting data in a batch mode. All the data provided with the [CottontailGrpc.InsertMessage] will be inserted
     * in a single transaction. I.e. either the insert succeeds or fails completely.
     */
    override fun insert(request: CottontailGrpc.InsertMessage, responseObserver: StreamObserver<CottontailGrpc.InsertStatus>) = try {
        val schema = this.catalogue.getSchema(request.entity.schema.name)
        val entity = schema.getEntity(request.entity.name)
        entity.Tx(false).begin { tx ->
            request.tupleList.map { it.dataMap }.forEach {
                val columns = it.map { col -> entity.columnForName(col.key) ?: throw DatabaseException.ColumnNotExistException(col.key, entity.name) }.toTypedArray()
                val values = it.map { col -> col.value }
                tx.insert(StandaloneRecord(columns = columns, init = values.toTypedArray()))
            }
            true
        }
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
                request.tupleList.map { it.dataMap }.forEach {
                    val columns = it.map { col -> entity.columnForName(col.key) ?: throw DatabaseException.ColumnNotExistException(col.key, entity.name) }.toTypedArray()
                    val values = it.map { col -> col.value }
                    tx.insert(StandaloneRecord(columns = columns, init = values.toTypedArray()))
                }
                true
            }
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
}