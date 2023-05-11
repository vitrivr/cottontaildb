package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc.Type

/**
 * A CREATE ENTITY query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class CreateEntity(name: Name.EntityName): LanguageFeature() {

    constructor(name: String): this(Name.EntityName.parse(name))

    /** Internal [CottontailGrpc.CreateEntityMessage.Builder]. */
    internal val builder = CottontailGrpc.CreateEntityMessage.newBuilder().setEntity(name.proto())

    /**
     * Sets the transaction ID for this [CreateEntity].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): CreateEntity {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [CreateEntity].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): CreateEntity {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [CreateEntity]
     *
     * @return The size in bytes of this [CreateEntity].
     */
    override fun serializedSize() = this.builder.build().serializedSize

    /**
     * Adds a column to this [CreateEntity].
     *
     * @param name The name of the column.
     * @param type The [Types] of the column.
     * @param nullable Flag indicating whether column should be nullable.
     * @param autoIncrement Flag indicating whether column should be auto incremented. Only works for [Type.INTEGER] or [Type.LONG]
     * @return this [CreateEntity]
     */
    fun column(name: Name.ColumnName, type: Types<*>, nullable: Boolean = false, autoIncrement: Boolean = false): CreateEntity {
        val addBuilder = builder.addColumnsBuilder()
        addBuilder.name = name.proto()
        addBuilder.type = Type.valueOf(type.name)
        addBuilder.length = type.logicalSize
        addBuilder.nullable = nullable
        if (autoIncrement) {
            require(type == Types.Int || type == Types.Long) { "Auto-increment option is only supported by INTEGER and LONG columns."}
            addBuilder.autoIncrement = true
        }
        return this
    }

    /**
     * Adds a column to this [CreateEntity].
     *
     * @param name The name of the column.
     * @param type The [CottontailGrpc.Type] of the column (as string).
     * @param length The length of the column (>= 1 for vector columns)
     * @param nullable Flag indicating whether column should be nullable.
     * @return this [CreateEntity]
     */
    fun column(name: String, type: String, length: Int = 0, nullable: Boolean = false, autoIncrement: Boolean = false)
        = this.column(Name.ColumnName.parse(name), Types.forName(type.uppercase(), length), nullable, autoIncrement)
}