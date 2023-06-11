package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A DROP SCHEMA query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class DropSchema(name: Name.SchemaName): LanguageFeature() {

    constructor(name: String): this(Name.SchemaName.parse(name))

    /** Internal [CottontailGrpc.DropSchemaMessage.Builder]. */
    internal val builder = CottontailGrpc.DropSchemaMessage.newBuilder().setSchema(name.proto())

    /**
     * Sets the transaction ID for this [DropSchema].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): DropSchema {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [DropSchema].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): DropSchema {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [DropSchema]
     *
     * @return The size in bytes of this [DropSchema].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}