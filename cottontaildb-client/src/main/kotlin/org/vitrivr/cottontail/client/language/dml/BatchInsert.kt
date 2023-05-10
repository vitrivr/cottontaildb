package org.vitrivr.cottontail.client.language.dml

import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.core.tryConvertToValue
import org.vitrivr.cottontail.core.values.PublicValue
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A BATCH INSERT query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class BatchInsert(entity: String? = null): LanguageFeature() {
    /** Internal [CottontailGrpc.DeleteMessage.Builder]. */
    internal val builder = CottontailGrpc.BatchInsertMessage.newBuilder()

    init {
        if (entity != null) {
            this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        }
    }

    /**
     * Sets the transaction ID for this [BatchInsert].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): BatchInsert {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [BatchInsert].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): BatchInsert {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [BatchInsert]
     *
     * @return The size in bytes of this [BatchInsert].
     */
    override fun serializedSize() = this.builder.build().serializedSize

    /**
     * Adds a FROM-clause to this [BatchInsert].
     *
     * @param entity The name of the entity to [BatchInsert] to.
     * @return This [BatchInsert]
     */
    fun into(entity: String): BatchInsert {
        this.builder.clearFrom()
        this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        return this
    }

    /**
     * Adds a column to this [BatchInsert].
     *
     * @param columns The name of the columns this [BatchInsert] should insert into.
     * @return This [BatchInsert]
     */
    fun columns(vararg columns: String): BatchInsert {
        this.builder.clearColumns()
        for (c in columns) {
            this.builder.addColumns(c.parseColumn())
        }
        return this
    }
    /**
     * Appends values to this [BatchInsert].
     *
     * @param values The value to append to the [BatchInsert]
     * @return This [BatchInsert]
     */
    fun any(vararg values: Any?): Boolean = this.values(*values.map { it?.tryConvertToValue() }.toTypedArray())


    /**
     * Appends values to this [BatchInsert].
     *
     * @param values The [PublicValue]s to append to the [BatchInsert]
     * @return This [BatchInsert]
     */
    fun values(vararg values: PublicValue?): Boolean {
        val insert = CottontailGrpc.BatchInsertMessage.Insert.newBuilder()
        for (v in values) {
            insert.addValues(v?.toGrpc() ?: CottontailGrpc.Literal.newBuilder().build())
        }
        val built = insert.build()
        return if (this.serializedSize() + built.serializedSize < Constants.MAX_PAGE_SIZE_BYTES) {
            this.builder.addInserts(built)
            true
        } else {
            false
        }
    }

    /**
     * Clears all appended data from this [BatchInsert] object. Making it possible to re-use the same object to perform multiple INSERTs.
     */
    fun clear() {
        this.builder.clearInserts()
    }

    /**
     * Returns the number of insert values in this [BatchInsert] message.
     *
     * @return The number of insert values in this [BatchInsert] message.
     */
    fun count() = this.builder.insertsCount
}