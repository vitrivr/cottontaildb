package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An ANALYZE ENTITY query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class AnalyzeEntity(name: String): LanguageFeature() {
    /** Internal [CottontailGrpc.AnalyzeEntityMessage.Builder]. */
    internal val builder = CottontailGrpc.AnalyzeEntityMessage.newBuilder()

    init {
        builder.entity = name.parseEntity()
    }

    /**
     * Sets the transaction ID for this [AnalyzeEntity].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): AnalyzeEntity {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [AnalyzeEntity].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): AnalyzeEntity {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Sets the asynchronous flag for this [AnalyzeEntity].
     *
     * @return This [AnalyzeEntity]
     */
    fun async(): AnalyzeEntity {
        this.builder.async = true
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [AnalyzeEntity]
     *
     * @return The size in bytes of this [AnalyzeEntity].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}