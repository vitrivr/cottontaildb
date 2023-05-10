package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to query information about a column.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class EntityStatistics(name: String): LanguageFeature() {
    /** Internal [CottontailGrpc.ColumnDetailsMessage.Builder]. */
    internal val builder = CottontailGrpc.EntityDetailsMessage.newBuilder()

    init {
        this.builder.entity = name.parseEntity()
    }

    /**
     * Sets the transaction ID for this [EntityStatistics].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): EntityStatistics {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [EntityStatistics].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): EntityStatistics {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [EntityStatistics]
     *
     * @return The size in bytes of this [EntityStatistics].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}