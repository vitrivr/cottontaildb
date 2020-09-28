package org.vitrivr.cottontail.server.grpc.services

import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.Transaction
import org.vitrivr.cottontail.grpc.CottonDMLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.ValidationException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.server.grpc.helper.fqn
import org.vitrivr.cottontail.server.grpc.helper.toValue
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.StampedLock

/**
 * Implementation of [CottonDMLGrpc.CottonDMLImplBase], the gRPC endpoint for inserting data into Cottontail DB [Entity]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class CottonDMLService(val catalogue: Catalogue) : CottonDMLGrpc.CottonDMLImplBase() {
    /** Logger used for logging the output. */
    companion object {
        private val LOGGER = LoggerFactory.getLogger(CottonDMLService::class.java)
    }

    /**
     * gRPC endpoint for inserting data in a streaming mode; transactions will stay open until the caller explicitly completes or until an error occurs.
     * As new entities are being inserted, new transactions will be created and thus new locks will be acquired.
     */
    override fun insert(responseObserver: StreamObserver<CottontailGrpc.Status>): StreamObserver<CottontailGrpc.InsertMessage> = InsertSink(responseObserver)

    /**
     * Class that acts as [StreamObserver] for [CottontailGrpc.InsertMessage]s.
     *
     * @author Ralph Gasser
     * @version 1.0
     */
    inner class InsertSink(private val responseObserver: StreamObserver<CottontailGrpc.Status>) : StreamObserver<CottontailGrpc.InsertMessage> {

        /** List of all the [Entity.Tx] associated with this call. */
        private val transactions = ConcurrentHashMap<Name.EntityName, Entity.Tx>()

        /** Generates a new transaction id. */
        private val txId = UUID.randomUUID()

        /** Lock for closing this */
        private val closeLock = StampedLock()

        /** Internal counter used to keep track of how many items were inserted. */
        @Volatile
        private var counter: Long = 0

        /** Flag indicating that call was closed. */
        @Volatile
        private var closed = false

        init {
            LOGGER.trace("Insert transaction {} was initiated by client.", txId.toString())
        }

        /**
         * Called whenever a new [CottontailGrpc.InsertMessage] arrives.
         *
         * @param request The next [CottontailGrpc.InsertMessage] message.
         */
        override fun onNext(request: CottontailGrpc.InsertMessage) {
            try {
                /* Check if call was closed and return. */
                this.closeLock.read {
                    if (this.closed) return
                    val fqn = try {
                        request.from.entity.fqn()
                    } catch (e: IllegalArgumentException) {
                        responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Failed to insert into entity: ${e.message}").asException())
                        return
                    }

                    /* Re-use or create Transaction. */
                    var tx = this.transactions[fqn]
                    if (tx == null) {
                        /* Extract required schema and entity. */
                        val schema = this@CottonDMLService.catalogue.schemaForName(fqn.schema())
                        val entity = schema.entityForName(fqn)
                        tx = entity.Tx(false, this.txId)
                        this.transactions[fqn] = tx
                    }

                    /* Execute insert action. */
                    val columns = ArrayList<ColumnDef<*>>(request.tuple.dataMap.size)
                    val values = ArrayList<Value?>(request.tuple.dataMap.size)
                    request.tuple.dataMap.forEach {
                        val col = tx.entity.columnForName(fqn.column(it.key))
                                ?: throw ValidationException("INSERT failed because column ${it.key} does not exist in entity '$fqn'.")
                        columns.add(col)
                        values.add(it.value.toValue(col))
                    }

                    /* Conduct INSERT. */
                    tx.insert(StandaloneRecord(columns = columns.toTypedArray(), values = values.toTypedArray()))

                    /* Increment counter. */
                    this.counter += 1

                    /* Respond with status. */
                    this.responseObserver.onNext(CottontailGrpc.Status.newBuilder().setSuccess(true).setTimestamp(System.currentTimeMillis()).build())
                }
            } catch (e: DatabaseException.SchemaDoesNotExistException) {
                this.cleanup(false)
                this.responseObserver.onError(Status.NOT_FOUND.withDescription("INSERT failed because schema '${request.from.entity.schema.name} does not exist!").asException())
            } catch (e: DatabaseException.EntityDoesNotExistException) {
                this.cleanup(false)
                this.responseObserver.onError(Status.NOT_FOUND.withDescription("INSERT failed because entity '${request.from.entity.fqn()} does not exist!").asException())
            } catch (e: DatabaseException.ColumnDoesNotExistException) {
                this.cleanup(false)
                this.responseObserver.onError(Status.NOT_FOUND.withDescription("INSERT failed because column '${e.column}' does not exist!").asException())
            } catch (e: ValidationException) {
                this.cleanup(false)
                this.responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("INSERT failed because data validation failed: ${e.message}").asException())
            } catch (e: DatabaseException) {
                this.cleanup(false)
                this.responseObserver.onError(Status.INTERNAL.withDescription("INSERT failed because of a database error: ${e.message}").asException())
            } catch (e: Throwable) {
                this.cleanup(false)
                this.responseObserver.onError(Status.UNKNOWN.withDescription("INSERT failed because of a unknown error: ${e.message}").asException())
            }
        }

        /**
         * Called when the client-side indicates an error condition.
         */
        override fun onError(t: Throwable) {
            this.cleanup(false)
            this.responseObserver.onError(Status.ABORTED.withDescription("Transaction was aborted by client.").asException())
        }

        /**
         * Called when client-side completes invocation, effectively ending the [Transaction].
         */
        override fun onCompleted() {
            this.cleanup(true)
            this.responseObserver.onCompleted()
        }

        /**
         * Commits or rolls back all the  [Transaction]s associated with this call and closes them afterwards.
         *
         * @param commit If true [Transaction]s are commited before closing. Otherwise, they are rolled back.
         */
        private fun cleanup(commit: Boolean = false) = this.closeLock.write {
            this.closed = true
            this.transactions.forEach {
                try {
                    if (commit) {
                        it.value.commit()
                        LOGGER.trace("Insert transaction ${this.txId} was committed by client (${this.counter} tuples inserted).")
                    } else {
                        LOGGER.trace("Insert transaction ${this.txId} was rolled back by client.")
                        it.value.rollback()
                    }
                } finally {
                    it.value.close()
                }
            }
        }
    }
}
