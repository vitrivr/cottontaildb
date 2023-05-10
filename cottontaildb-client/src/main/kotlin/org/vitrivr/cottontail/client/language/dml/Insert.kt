package org.vitrivr.cottontail.client.language.dml

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.core.tryConvertToValue
import org.vitrivr.cottontail.core.values.PublicValue
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An INSERT query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class Insert(entity: String? = null): LanguageFeature() {
    /** Internal [CottontailGrpc.InsertMessage.Builder]. */
    internal val builder = CottontailGrpc.InsertMessage.newBuilder()

    init {
        if (entity != null) {
            this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        }
    }

    /**
     * Sets the transaction ID for this [Update].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): Insert {
        this.builder.metadataBuilder.transactionId= txId
        return this
    }

    /**
     * Sets the query ID for this [Update].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): Insert {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [Insert]
     *
     * @return The size in bytes of this [Insert].
     */
    override fun serializedSize() = this.builder.build().serializedSize

    /**
     * Adds a FROM-clause to this [Insert].
     *
     * @param entity The name of the entity to [Insert] to.
     * @return This [Insert]
     */
    fun into(entity: String): Insert {
        this.builder.clearFrom()
        this.builder.setFrom(
            CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        return this
    }

    /**
     * Adds a value assignments this [Insert]. This method is cumulative, i.e., invoking
     * this method multiple times appends another assignment each time.
     *
     * @param column The name of the column to insert into.
     * @param value The value or null.
     * @return This [Insert]
     */
    fun any(column: String, value: Any?): Insert = this.value(column, value?.tryConvertToValue())

    /**
     * Adds a value assignments this [Insert]. This method is cumulative, i.e., invoking
     * this method multiple times appends another assignment each time.
     *
     * @param column The name of the column to insert into.
     * @param value The value or null.
     * @return This [Insert]
     */
    fun value(column: String, value: PublicValue?): Insert {
        this.builder.addElements(
            CottontailGrpc.InsertMessage.InsertElement.newBuilder()
                .setColumn(column.parseColumn())
                .setValue(value?.toGrpc() ?: CottontailGrpc.Literal.newBuilder().build()))
        return this
    }

    /**
     * Adds value assignments this [Insert]. A value assignment consists of a column name and a value.
     *
     * @param assignments The value assignments for the [Insert]
     * @return This [Insert]
     */
    fun any(vararg assignments: Pair<String, Any?>): Insert {
        for (assignment in assignments) {
            this.value(assignment.first, assignment.second?.tryConvertToValue())
        }
        return this
    }

    /**
     * Adds value assignments this [Insert]. A value assignment consists of a column name and a value.
     *
     * @param assignments The value assignments for the [Insert]
     * @return This [Insert]
     */
    fun values(vararg assignments: Pair<String, PublicValue?>): Insert {
        for (assignment in assignments) {
            this.value(assignment.first, assignment.second)
        }
        return this
    }
}