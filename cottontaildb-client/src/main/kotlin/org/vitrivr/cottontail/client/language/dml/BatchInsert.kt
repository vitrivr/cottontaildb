package org.vitrivr.cottontail.client.language.dml

import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.proto
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tryConvertToValue
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A BATCH INSERT query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class BatchInsert(entity: Name.EntityName): LanguageFeature() {

    constructor(entity: String): this(Name.EntityName.parse(entity))

    /** Internal [CottontailGrpc.DeleteMessage.Builder]. */
    internal val builder = CottontailGrpc.BatchInsertMessage.newBuilder()

    /** */
    private var payloadSize = 0L

    init {
        this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.proto())))
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
     * Adds a column to this [BatchInsert].
     *
     * @param columns The name of the columns this [BatchInsert] should insert into.
     * @return This [BatchInsert]
     */
    fun columns(vararg columns: Name.ColumnName): BatchInsert {
        this.builder.clearColumns()
        for (c in columns) {
            this.builder.addColumns(c.proto())
        }
        return this
    }

    /**
     * Adds a column to this [BatchInsert].
     *
     * @param columns The name of the columns this [BatchInsert] should insert into.
     * @return This [BatchInsert]
     */
    fun columns(columns: Iterable<Name.ColumnName>): BatchInsert {
        this.builder.clearColumns()
        for (c in columns) {
            this.builder.addColumns(c.proto())
        }
        return this
    }

    /**
     * Adds a column to this [BatchInsert].
     *
     * @param columns The name of the columns this [BatchInsert] should insert into.
     * @return This [BatchInsert]
     */
    fun columns(vararg columns: String): BatchInsert = this.columns(*columns.map { Name.ColumnName.parse(it) }.toTypedArray())

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
     * @param values The [Value]s to append to the [BatchInsert]
     * @return This [BatchInsert]
     */
    fun values(vararg values: Value?): Boolean {
        val insert = CottontailGrpc.BatchInsertMessage.Insert.newBuilder()
        for (value in values) {
            insert.addValues(value?.toGrpc() ?: CottontailGrpc.Literal.newBuilder().setNullData(CottontailGrpc.Null.newBuilder()).build())
            this.payloadSize += value?.physicalSize ?: 0
        }
        return if (this.payloadSize < Constants.MAX_PAGE_SIZE_BYTES) {
            this.builder.addInserts(insert)
            true
        } else {
            false
        }
    }

    /**
     * Appends values to this [BatchInsert].
     *
     * @param values The [Value]s to append to the [BatchInsert]
     * @return This [BatchInsert]
     */
    fun values(values: Iterable<Value?>): Boolean {
        val insert = CottontailGrpc.BatchInsertMessage.Insert.newBuilder()
        for (value in values) {
            insert.addValues(value?.toGrpc() ?: CottontailGrpc.Literal.newBuilder().setNullData(CottontailGrpc.Null.newBuilder()).build())
            this.payloadSize += value?.physicalSize ?: 0
        }
        return if (this.payloadSize < Constants.MAX_PAGE_SIZE_BYTES) {
            this.builder.addInserts(insert)
            true
        } else {
            false
        }
    }

    /**
     * Clears all appended data from this [BatchInsert] object. Making it possible to re-use the same object to perform multiple INSERTs.
     */
    fun clear() {
        this.payloadSize = 0
        this.builder.clearInserts()
    }

    /**
     * Returns the number of insert values in this [BatchInsert] message.
     *
     * @return The number of insert values in this [BatchInsert] message.
     */
    fun count() = this.builder.insertsCount
}