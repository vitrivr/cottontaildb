package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to list all schemas.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class ListSchemas: LanguageFeature() {
    /** Internal [CottontailGrpc.ListSchemaMessage.Builder]. */
    internal val builder = CottontailGrpc.ListSchemaMessage.newBuilder()

    /**
     * Sets the transaction ID for this [ListSchemas].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): ListSchemas {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [ListSchemas].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): ListSchemas {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [ListSchemas]
     *
     * @return The size in bytes of this [ListSchemas].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}