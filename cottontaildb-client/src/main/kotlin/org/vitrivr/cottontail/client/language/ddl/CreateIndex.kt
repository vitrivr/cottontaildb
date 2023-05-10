package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A CREATE INDEX query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class CreateIndex(entity: String, column: String, type: CottontailGrpc.IndexType): LanguageFeature() {

    internal val builder = CottontailGrpc.CreateIndexMessage.newBuilder()

    init {
        require(!column.contains('.')) { "Column name must not contain any dots." }
        this.builder.entity = entity.parseEntity()
        this.builder.addColumns(column)
        this.builder.type = type
    }

    /**
     * Sets the transaction ID for this [CreateIndex].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): CreateIndex {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [CreateIndex].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): CreateIndex {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [CreateIndex]
     *
     * @return The size in bytes of this [CreateIndex].
     */
    override fun serializedSize() = this.builder.build().serializedSize

    /**
     * Adds a column to this [CreateIndex].
     *
     * @param column The name of the column
     * @return this [CreateIndex]
     */
    fun column(column: String): CreateIndex {
        require(!column.contains('.')) { "Column name must not contain any dots." }
        this.builder.addColumns(column)
        return this
    }

    /**
     * Sets the index name for this [CreateIndex].
     *
     * @param index The name of the index to be created
     * @return this [CreateIndex]
     */
    fun name(index: String): CreateIndex {
        require(!index.contains('.')) { "Index name must not contain any dots." }
        this.builder.indexName = index
        return this
    }

    /**
     * Adds an index creation parameter to this [CreateIndex].
     *
     * @param key The name of the parameter
     * @param value The value of the parameter
     * @return this [CreateIndex]
     */
    fun param(key: String, value: Any): CreateIndex {
        this.builder.putParams(key, value.toString())
        return this
    }
}