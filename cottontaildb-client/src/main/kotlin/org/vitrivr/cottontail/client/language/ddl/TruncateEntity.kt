package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A TRUNCATE ENTITY query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class TruncateEntity(name: String): LanguageFeature() {
    /** Internal [CottontailGrpc.DeleteMessage.Builder]. */
    internal val builder = CottontailGrpc.TruncateEntityMessage.newBuilder()

    init {
        builder.entity = name.parseEntity()
    }

    /**
     * Sets the transaction ID for this [DropEntity].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): TruncateEntity {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [DropEntity].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): TruncateEntity {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [TruncateEntity]
     *
     * @return The size in bytes of this [TruncateEntity].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}