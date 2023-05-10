package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseSchema
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A CREATE SCHEMA query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class CreateSchema(name: String): LanguageFeature() {
    /** Internal [CottontailGrpc.CreateSchemaMessage.Builder]. */
    internal val builder = CottontailGrpc.CreateSchemaMessage.newBuilder()

    init {
        this.builder.schema = name.parseSchema()
    }

    /**
     * Sets the transaction ID for this [CreateSchema].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): CreateSchema {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [CreateSchema].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): CreateSchema {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [CreateSchema]
     *
     * @return The size in bytes of this [CreateSchema].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}