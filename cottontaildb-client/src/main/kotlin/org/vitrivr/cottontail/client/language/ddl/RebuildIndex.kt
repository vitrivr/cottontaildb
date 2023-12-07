package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An REBUILD INDEX query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class RebuildIndex(name: Name.IndexName): LanguageFeature() {

    constructor(name: String): this(Name.IndexName.parse(name))

    /** Internal [CottontailGrpc.RebuildIndexMessage.Builder]. */
    internal val builder = CottontailGrpc.RebuildIndexMessage.newBuilder().setIndex(name.proto())

    /**
     * Sets the transaction ID for this [RebuildIndex].
     *
     * @param txId The new transaction ID.
     * @return This [RebuildIndex]
     */
    override fun txId(txId: Long): RebuildIndex {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [RebuildIndex].
     *
     * @param queryId The new query ID.
     * @return This [RebuildIndex]
     */
    override fun queryId(queryId: String): RebuildIndex {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Sets the asynchronous flag for this [RebuildIndex].
     *
     * @return This [RebuildIndex]
     */
    fun async(): RebuildIndex {
        this.builder.async = true
        return this
    }
}