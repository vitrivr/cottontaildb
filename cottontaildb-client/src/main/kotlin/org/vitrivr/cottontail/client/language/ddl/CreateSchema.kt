package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A CREATE SCHEMA query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class CreateSchema(name: Name.SchemaName): LanguageFeature() {

    constructor(name: String): this(Name.SchemaName.parse(name))

    /** Internal [CottontailGrpc.CreateSchemaMessage.Builder]. */
    internal val builder = CottontailGrpc.CreateSchemaMessage.newBuilder().setSchema(name.proto())

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
     * Sets the IF NOT EXISTS flag. Means that the request will fail gracefully if the schema already exists.
     *
     * @return The [CreateEntity] object.
     */
    fun ifNotExists(): CreateSchema {
        this.builder.mayExist = true
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [CreateSchema]
     *
     * @return The size in bytes of this [CreateSchema].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}