package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseIndex
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A DROP INDEX query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class DropIndex(name: String): LanguageFeature() {
    internal val builder = CottontailGrpc.DropIndexMessage.newBuilder()

    init {
        this.builder.index = name.parseIndex()
    }

    /**
     * Sets the transaction ID for this [DropIndex].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): DropIndex {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [DropIndex].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): DropIndex {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [DropIndex]
     *
     * @return The size in bytes of this [DropIndex].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}