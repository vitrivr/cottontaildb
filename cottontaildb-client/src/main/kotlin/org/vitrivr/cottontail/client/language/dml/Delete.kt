package org.vitrivr.cottontail.client.language.dml

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.basics.predicate.*
import org.vitrivr.cottontail.client.language.extensions.*
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A DELETE query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class Delete(entity: String? = null): LanguageFeature() {
    /** Internal [CottontailGrpc.DeleteMessage.Builder]. */
    internal val builder = CottontailGrpc.DeleteMessage.newBuilder()

    init {
        if (entity != null) {
            this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        }
    }

    /**
     * Sets the transaction ID for this [Delete].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): Delete {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [Delete].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): Delete {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [Delete]
     *
     * @return The size in bytes of this [Delete].
     */
    override fun serializedSize() = this.builder.build().serializedSize

    /**
     * Adds a FROM-clause to this [Delete].
     *
     * @param entity The name of the entity to [Delete] from.
     * @return This [Delete]
     */
    fun from(entity: String): Delete {
        this.builder.clearFrom()
        this.builder.setFrom(
            CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        return this
    }

    /**
     * Adds a WHERE-clause to this [Delete].
     *
     * @return This [Delete]
     */
    fun where(predicate: Predicate): Delete {
        this.builder.clearWhere()
        this.builder.whereBuilder.setPredicate(predicate.toGrpc())
        return this
    }
}