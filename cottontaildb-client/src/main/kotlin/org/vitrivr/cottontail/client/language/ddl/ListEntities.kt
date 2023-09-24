package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to list all entities in a schema.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class ListEntities(name: Name.SchemaName? = null): LanguageFeature() {

    constructor(name: String): this(Name.SchemaName.parse(name))

    /** Internal [CottontailGrpc.ListEntityMessage.Builder]. */
    internal val builder = CottontailGrpc.ListEntityMessage.newBuilder()

    init {
        if (name != null) {
            this.builder.setSchema(name.proto())
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
}