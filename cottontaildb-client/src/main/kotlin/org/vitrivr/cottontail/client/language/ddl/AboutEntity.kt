package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to query information about an entity.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class AboutEntity(name: String): LanguageFeature() {
    /** Internal [CottontailGrpc.EntityDetailsMessage.Builder]. */
    internal val builder = CottontailGrpc.EntityDetailsMessage.newBuilder()

    init {
        this.builder.entity = name.parseEntity()
    }

    /**
     * Sets the transaction ID for this [AboutEntity].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): AboutEntity {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [AboutEntity].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): AboutEntity {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [AboutEntity]
     *
     * @return The size in bytes of this [AboutEntity].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}