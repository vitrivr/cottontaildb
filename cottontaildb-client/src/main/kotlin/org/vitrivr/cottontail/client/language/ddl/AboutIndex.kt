package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to query information about an index.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class AboutIndex(name: Name.IndexName): LanguageFeature() {

    constructor(name: String): this(Name.IndexName.parse(name))

    /** Internal [CottontailGrpc.ListEntityMessage.Builder]. */
    internal val builder = CottontailGrpc.IndexDetailsMessage.newBuilder().setIndex(name.proto())

    /**
     * Sets the transaction ID for this [AboutIndex].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): AboutIndex {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [AboutIndex].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): AboutIndex {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }
}