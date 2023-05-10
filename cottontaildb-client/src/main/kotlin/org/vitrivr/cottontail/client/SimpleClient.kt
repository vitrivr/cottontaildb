package org.vitrivr.cottontail.client

import com.google.protobuf.Empty
import io.grpc.Context
import io.grpc.ManagedChannel
import io.grpc.StatusRuntimeException
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.iterators.TupleIteratorImpl
import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.client.language.dml.Insert
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.client.language.dml.Update
import org.vitrivr.cottontail.grpc.*
import kotlin.coroutines.cancellation.CancellationException

/**
 * A simple Cottontail DB client for querying, data management and data definition. Can work with [LanguageFeature]s
 * and classical [CottontailGrpc] messages.
 *
 * As opposed to the pure gRPC implementation, the [SimpleClient] offers some advanced functionality such
 * as a more convenient [TupleIterator] and cancelable queries.
 *
 * The [SimpleClient] wraps a [ManagedChannel]. It remains to the caller, to setup and close that [ManagedChannel].
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class SimpleClient(private val channel: ManagedChannel): AutoCloseable {

    /** [Context.CancellableContext] that can be used to cancel all queries that are currently being executed by this [SimpleClient]. */
    private val context = Context.current().withCancellation()

    /**
     * Begins a new transaction through this [SimpleClient].
     *
     * @param readonly Flag indicating whether a read-only or a read/write transaction should be started.
     * @return The ID of the newly begun transaction.
     */
    fun begin(readonly: Boolean = false): Long = TXNGrpc.newBlockingStub(this.channel).begin(
        if (readonly){
            CottontailGrpc.BeginTransaction.newBuilder().setMode(CottontailGrpc.TransactionMode.READONLY).build()
        } else {
            CottontailGrpc.BeginTransaction.newBuilder().setMode(CottontailGrpc.TransactionMode.READ_WRITE).build()
        }
    ).transactionId

    /**
     * Commits a transaction through this [SimpleClient].
     *
     * @param txId The transaction ID to commit.
     */
    fun commit(txId: Long) = this.context.run {
        val tx = CottontailGrpc.RequestMetadata.newBuilder().setTransactionId(txId).build()
        TXNGrpc.newBlockingStub(this.channel).commit(tx)
    }

    /**
     * Rolls back a transaction through this [SimpleClient].
     *
     * @param txId The transaction ID to rollback.
     */
    fun rollback(txId: Long) = this.context.run {
        val tx = CottontailGrpc.RequestMetadata.newBuilder().setTransactionId(txId).build()
        TXNGrpc.newBlockingStub(this.channel).rollback(tx)
    }

    /**
     * Kills and rolls back a transaction through this [SimpleClient].
     *
     * @param txId The transaction ID to kill and rollback.
     */
    fun kill(txId: Long) = this.context.run {
        val tx = CottontailGrpc.RequestMetadata.newBuilder().setTransactionId(txId).build()
        TXNGrpc.newBlockingStub(this.channel).kill(tx)
    }
    /**
     * Systems command to list all transaction executed by Cottontail DB.
     *
     * @return [TupleIterator]
     */
    fun transactions(): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = TXNGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.listTransactions(Empty.newBuilder().build()), inner)
        }
    }

    /**
     * Systems command to list all locks currently held by Cottontail DB.
     *
     * @return [TupleIterator]
     */
    fun locks(): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub =TXNGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.listLocks(Empty.newBuilder().build()), inner)
        }
    }

    /**
     * Executes [CottontailGrpc.Query] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.Query] to execute.
     * @return An [TupleIterator] of the results.
     */
    fun query(message: CottontailGrpc.QueryMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DQLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.query(message), inner)
        }
    }

    /**
     * Executes [Query] through this [SimpleClient]
     *
     * @param q [Query] to execute.
     * @return [TupleIterator] of the result.
     */
    fun query(q: Query): TupleIterator = this.query(q.builder.build())

    /**
     * Explains [CottontailGrpc.Query] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.Query] to executed.
     * @return An [Iterator] iof [CottontailGrpc.QueryResponseMessage]
     */
    fun explain(message: CottontailGrpc.QueryMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DQLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.explain(message), inner)
        }
    }

    /**
     * Executes [Query] through this [SimpleClient]
     *
     * @param q [Query] to execute.
     * @return [TupleIterator] of the result.
     */
    fun explain(q: Query): TupleIterator = this.explain(q.builder.build())

    /**
     * Executes this [CottontailGrpc.InsertMessage] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.InsertMessage] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun insert(message: CottontailGrpc.InsertMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DMLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.insert(message), inner)
        }
    }

    /**
     * Executes this [Insert] through this [SimpleClient]
     *
     * @return [TupleIterator] containing the query response.
     */
    fun insert(query: Insert): TupleIterator = this.insert(query.builder.build())

    /**
     * Executes this [CottontailGrpc.BatchInsertMessage] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.BatchInsertMessage] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun insert(message: CottontailGrpc.BatchInsertMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DMLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.insertBatch(message), inner)
        }
    }

    /**
     * Executes this [BatchInsert] through this [SimpleClient]
     *
     * @param query [BatchInsert] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun insert(query: BatchInsert): TupleIterator = this.insert(query.builder.build())

    /**
     * Executes this [CottontailGrpc.UpdateMessage] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.UpdateMessage] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun update(message: CottontailGrpc.UpdateMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DMLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.update(message), inner)
        }
    }

    /**
     * Executes this [Update] through this [SimpleClient]
     *
     * @param query [Update] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun update(query: Update): TupleIterator = this.update(query.builder.build())


    /**
     * Explains [CottontailGrpc.DeleteMessage] through this [SimpleClient]
     *
     * @param message [CottontailGrpc.DeleteMessage] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun delete(message: CottontailGrpc.DeleteMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DMLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.delete(message), inner)
        }
    }

    /**
     * Executes this [Delete] through this [SimpleClient]
     *
     * @param query [Delete] to execute.
     * @return [TupleIterator] containing the query response.
     */
    fun delete(query: Delete): TupleIterator = this.delete(query.builder.build())

    /**
     * Creates a new schema through this [SimpleClient].
     *
     * @param message [CottontailGrpc.CreateSchemaMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun create(message: CottontailGrpc.CreateSchemaMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.createSchema(message), inner)
        }
    }

    /**
     * Creates a new schema through this [SimpleClient].
     *
     * @param message [CreateSchema] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun create(message: CreateSchema): TupleIterator = this.create(message.builder.build())

    /**
     * Creates a new entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.CreateEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun create(message: CottontailGrpc.CreateEntityMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.createEntity(message), inner)
        }
    }

    /**
     * Creates a new entity through this [SimpleClient].
     *
     * @param message [CreateEntity] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun create(message: CreateEntity): TupleIterator = this.create(message.builder.build())

    /**
     * Creates a new index through this [SimpleClient].
     *
     * @param message [CottontailGrpc.CreateIndexMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun create(message: CottontailGrpc.CreateIndexMessage): TupleIterator = this.context.call {
        val stub = DDLGrpc.newBlockingStub(this.channel)
        val inner = Context.current().withCancellation()
        inner.call {
            TupleIteratorImpl(stub.createIndex(message), inner)
        }
    }

    /**
     * Creates a new index through this [SimpleClient].
     *
     * @param message [CreateIndex] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun create(message: CreateIndex): TupleIterator = this.create(message.builder.build())

    /**
     * Drops a schema through this [SimpleClient].
     *
     * @param message [CottontailGrpc.DropSchemaMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun drop(message: CottontailGrpc.DropSchemaMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.dropSchema(message), inner)
        }
    }

    /**
     * Drops a schema through this [SimpleClient].
     *
     * @param message [CreateIndex] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun drop(message: DropSchema): TupleIterator = this.drop(message.builder.build())

    /**
     * Drops an entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.DropEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun drop(message: CottontailGrpc.DropEntityMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.dropEntity(message), inner)
        }
    }

    /**
     * Drops an entity through this [SimpleClient].
     *
     * @param message [DropEntity] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun drop(message: DropEntity): TupleIterator = this.drop(message.builder.build())

    /**
     * Drops an index through this [SimpleClient].
     *
     * @param message [CottontailGrpc.DropIndexMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun drop(message: CottontailGrpc.DropIndexMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.dropIndex(message), inner)
        }
    }

    /**
     * Drops an index through this [SimpleClient].
     *
     * @param message [DropIndex] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun drop(message: DropIndex): TupleIterator = this.drop(message.builder.build())

    /**
     * Lists all schemas through this [SimpleClient].
     *
     * @param message [CottontailGrpc.ListSchemaMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun list(message: CottontailGrpc.ListSchemaMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.listSchemas(message), inner)
        }
    }

    /**
     * Lists all schemas through this [SimpleClient].
     *
     * @param message [ListSchemas] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun list(message: ListSchemas): TupleIterator = this.list(message.builder.build())

    /**
     * Lists all entities in a schema through this [SimpleClient].
     *
     * @param message [CottontailGrpc.ListEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun list(message: CottontailGrpc.ListEntityMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.listEntities(message), inner)
        }
    }

    /**
     * Lists all entities in a schema through this [SimpleClient].
     *
     * @param message [ListEntities] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun list(message: ListEntities): TupleIterator = this.list(message.builder.build())

    /**
     * Lists detailed information about an entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.EntityDetailsMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun about(message: CottontailGrpc.EntityDetailsMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.entityDetails(message), inner)
        }
    }

    /**
     * Lists detailed information about an entity through this [SimpleClient].
     *
     * @param message [AboutEntity] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun about(message: AboutEntity): TupleIterator = this.about(message.builder.build())

    /**
     * Lists detailed information about an entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.ColumnDetailsMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun statistics(message: CottontailGrpc.EntityDetailsMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.entityStatistics(message), inner)
        }
    }

    /**
     * Lists detailed information about an entity through this [SimpleClient].
     *
     * @param message [EntityStatistics] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun about(message: EntityStatistics): TupleIterator = this.statistics(message.builder.build())

    /**
     * Lists detailed information about an entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.IndexDetailsMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun about(message: CottontailGrpc.IndexDetailsMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.indexDetails(message), inner)
        }
    }

    /**
     * Lists detailed information about an entity through this [SimpleClient].
     *
     * @param message [AboutIndex] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun about(message: AboutIndex): TupleIterator = this.about(message.builder.build())

    /**
     * Truncates the given entity through this [SimpleClient].
     *
     * @param message [TruncateEntity] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun truncate(message: TruncateEntity): TupleIterator = this.truncate(message.builder.build())

    /**
     * Truncates the given entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.OptimizeEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun truncate(message: CottontailGrpc.TruncateEntityMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.truncateEntity(message), inner)
        }
    }

    /**
     * Analyzes an entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.AnalyzeEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun analyze(message: CottontailGrpc.AnalyzeEntityMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.analyzeEntity(message), inner)
        }
    }

    /**
     * Rebuilds an index through this [SimpleClient].
     *
     * @param message [CottontailGrpc.AnalyzeEntityMessage] to execute.
     * @return [TupleIterator] containing the response.
     */
    fun rebuild(message: CottontailGrpc.RebuildIndexMessage): TupleIterator = this.context.call {
        val inner = Context.current().withCancellation()
        inner.call {
            val stub = DDLGrpc.newBlockingStub(this.channel)
            TupleIteratorImpl(stub.rebuildIndex(message), inner)
        }
    }

    /**
     * Rebuilds an index through this [SimpleClient].
     *
     * @param message [RebuildIndex] to execute.
     * @return [TupleIterator]
     */
    fun rebuild(message: RebuildIndex): TupleIterator = this.rebuild(message.builder.build())

    /**
     * Analyzes an entity through this [SimpleClient].
     *
     * @param message [AnalyzeEntity] to execute.
     * @return [TupleIterator]
     */
    fun analyze(message: AnalyzeEntity): TupleIterator = this.analyze(message.builder.build())

    /**
     * Pings this Cottontail DB instance. The method returns true on success and false otherwise.
     *
     * @return true on success, false otherwise.
     */
    fun ping(): Boolean = try {
        DQLGrpc.newBlockingStub(this.channel).ping(Empty.getDefaultInstance())
        true
    } catch (e: StatusRuntimeException) {
        false
    }

    /**
     * Closes this [SimpleClient].
     */
    override fun close() {
        if (!this.context.isCancelled) {
            this.context.cancel(CancellationException("Cottontail DB client was closed by the user."))
        }
    }
}