package org.vitrivr.cottontail.client.language.dml

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tryConvertToValue
import org.vitrivr.cottontail.core.values.PublicValue
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An INSERT query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class Insert(entity: Name.EntityName): LanguageFeature() {

    constructor(entity: String): this(Name.EntityName.parse(entity))

    /** Internal [CottontailGrpc.InsertMessage.Builder]. */
    internal val builder = CottontailGrpc.InsertMessage.newBuilder()

    init {
        this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.proto())))
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
                .setColumn(Name.ColumnName.parse(column).proto())
                .setValue(value?.toGrpc() ?: CottontailGrpc.Literal.newBuilder().setNullData(CottontailGrpc.Null.newBuilder()).build()))
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