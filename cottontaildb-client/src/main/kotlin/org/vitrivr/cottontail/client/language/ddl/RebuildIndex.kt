package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseIndex
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An REBUILD INDEX query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class RebuildIndex(name: String): LanguageFeature() {
    /** Internal [CottontailGrpc.RebuildIndexMessage.Builder]. */
    internal val builder = CottontailGrpc.RebuildIndexMessage.newBuilder()

    init {
        this.builder.index = name.parseIndex()
    }

    /**
     * Sets the transaction ID for this [RebuildIndex].
     *
     * @param txId The new transaction ID.
     * @return This [RebuildIndex]
     */
    override fun txId(txId: Long): RebuildIndex {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [RebuildIndex].
     *
     * @param queryId The new query ID.
     * @return This [RebuildIndex]
     */
    override fun queryId(queryId: String): RebuildIndex {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Sets the asynchronous flag for this [RebuildIndex].
     *
     * @return This [RebuildIndex]
     */
    fun async(): RebuildIndex {
        this.builder.async = true
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [RebuildIndex]
     *
     * @return The size in bytes of this [RebuildIndex].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}