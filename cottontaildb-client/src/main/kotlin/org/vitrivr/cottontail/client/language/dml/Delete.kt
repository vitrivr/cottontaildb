package org.vitrivr.cottontail.client.language.dml

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.basics.predicate.Predicate
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A DELETE query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class Delete(entity: Name.EntityName): LanguageFeature() {

    constructor(entity: String): this(Name.EntityName.parse(entity))

    /** Internal [CottontailGrpc.DeleteMessage.Builder]. */
    internal val builder = CottontailGrpc.DeleteMessage.newBuilder()

    init {
        this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.proto())))
    }

    /**
     * Sets the transaction ID for this [Delete].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): Delete {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [Delete].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): Delete {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [Delete]
     *
     * @return The size in bytes of this [Delete].
     */
    override fun serializedSize() = this.builder.build().serializedSize

    /**
     * Adds a WHERE-clause to this [Delete].
     *
     * @return This [Delete]
     */
    fun where(predicate: Predicate): Delete {
        this.builder.clearWhere()
        this.builder.whereBuilder.setPredicate(predicate.toGrpc())
        return this
    }
}