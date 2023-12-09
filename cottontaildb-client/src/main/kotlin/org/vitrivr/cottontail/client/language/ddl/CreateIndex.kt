package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A CREATE INDEX query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class CreateIndex(name: Name.EntityName, type: CottontailGrpc.IndexType): LanguageFeature() {

    constructor (entity: String, type: CottontailGrpc.IndexType): this(Name.EntityName.parse(entity), type)

    internal val builder = CottontailGrpc.CreateIndexMessage.newBuilder()

    init {
        this.builder.entity = name.proto()
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
     * Adds a column to this [CreateIndex].
     *
     * @param column The name of the column
     * @return this [CreateIndex]
     */
    fun column(column: Name.ColumnName): CreateIndex {
        this.builder.addColumns(column.column)
        return this
    }

    /**
     * Adds a column to this [CreateIndex].
     *
     * @param column The name of the column
     * @return this [CreateIndex]
     */
    fun column(column: String): CreateIndex {
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