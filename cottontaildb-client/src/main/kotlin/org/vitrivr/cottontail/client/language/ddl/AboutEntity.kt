package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to query information about an entity.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class AboutEntity(name: Name.EntityName): LanguageFeature() {

    constructor(name: String): this(Name.EntityName.parse(name))

    /** Internal [CottontailGrpc.EntityDetailsMessage.Builder]. */
    internal val builder = CottontailGrpc.EntityDetailsMessage.newBuilder().setEntity(name.proto())

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