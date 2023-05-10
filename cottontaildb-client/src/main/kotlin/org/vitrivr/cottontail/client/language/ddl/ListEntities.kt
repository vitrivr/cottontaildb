package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseSchema
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to list all entities in a schema.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class ListEntities(schemaName: String? = null): LanguageFeature() {

    /** Internal [CottontailGrpc.ListEntityMessage.Builder]. */
    internal val builder = CottontailGrpc.ListEntityMessage.newBuilder()

    init {
        if (schemaName != null) {
            this.builder.schema = schemaName.parseSchema()
        }
    }

    /**
     * Sets the transaction ID for this [ListEntities].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): ListEntities {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [ListEntities].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): ListEntities {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [ListEntities]
     *
     * @return The size in bytes of this [ListEntities].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}