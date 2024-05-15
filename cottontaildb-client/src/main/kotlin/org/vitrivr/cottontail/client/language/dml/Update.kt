package org.vitrivr.cottontail.client.language.dml

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.basics.predicate.Predicate
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tryConvertToValue
import org.vitrivr.cottontail.core.values.PublicValue
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An UPDATE query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class Update(entity: Name.EntityName): LanguageFeature() {

    constructor(entity: String): this(Name.EntityName.parse(entity))

    /** Internal [CottontailGrpc.DeleteMessage.Builder]. */
    internal val builder = CottontailGrpc.UpdateMessage.newBuilder()

    init {
        this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.proto())))
    }

    /**
     * Sets the transaction ID for this [Update].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): Update {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [Update].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): Update {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Adds a WHERE-clause to this [Update].
     *
     * @param predicate The [Predicate] that specifies the conditions that need to be met for an [Update].
     * @return This [Update]
     */
    fun where(predicate: Predicate): Update {
        this.builder.clearWhere()
        this.builder.whereBuilder.setPredicate(predicate.toGrpc())
        return this
    }

    /**
     * Adds value assignments this [Update]
     *
     * @param assignments The value assignments for the [Update]
     * @return This [Update]
     */
    fun any(vararg assignments: Pair<String, Any?>): Update {
        for (assignment in assignments) {
            this.builder.addUpdates(
                CottontailGrpc.UpdateMessage.UpdateElement.newBuilder()
                    .setColumn(Name.ColumnName.parse(assignment.first).proto())
                    .setValue(CottontailGrpc.Expression.newBuilder().setLiteral(assignment.second?.tryConvertToValue()?.toGrpc() ?: CottontailGrpc.Literal.newBuilder().setNullData(CottontailGrpc.Null.newBuilder())))
            )
        }
        return this
    }

    /**
     * Adds value assignments this [Update]
     *
     * @param assignments The value assignments for the [Update]
     * @return This [Update]
     */
    fun values(vararg assignments: Pair<String, PublicValue?>): Update {
        for (assignment in assignments) {
            this.builder.addUpdates(
                CottontailGrpc.UpdateMessage.UpdateElement.newBuilder()
                    .setColumn(Name.ColumnName.parse(assignment.first).proto())
                .setValue(CottontailGrpc.Expression.newBuilder().setLiteral(assignment.second?.toGrpc() ?: CottontailGrpc.Literal.newBuilder().setNullData(CottontailGrpc.Null.newBuilder())))
            )
        }
        return this
    }
}