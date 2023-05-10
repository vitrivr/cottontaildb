package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseIndex
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to query information about an index.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AboutIndex(name: String): LanguageFeature() {
    /** Internal [CottontailGrpc.ListEntityMessage.Builder]. */
    internal val builder = CottontailGrpc.IndexDetailsMessage.newBuilder()

    init {
        this.builder.index = name.parseIndex()
    }

    /**
     * Sets the transaction ID for this [AboutIndex].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): AboutIndex {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [AboutIndex].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): AboutIndex {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [AboutIndex]
     *
     * @return The size in bytes of this [AboutIndex].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}