package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A DROP ENTITY query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class DropEntity(name: String): LanguageFeature() {
    /** Internal [CottontailGrpc.DeleteMessage.Builder]. */
    internal val builder = CottontailGrpc.DropEntityMessage.newBuilder()

    init {
        builder.entity = name.parseEntity()
    }

    /**
     * Sets the transaction ID for this [DropEntity].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): DropEntity {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [DropEntity].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): DropEntity {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [DropEntity]
     *
     * @return The size in bytes of this [DropEntity].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}