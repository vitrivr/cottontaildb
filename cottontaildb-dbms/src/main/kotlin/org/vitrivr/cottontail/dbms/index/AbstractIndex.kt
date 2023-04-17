package org.vitrivr.cottontail.dbms.index

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.ColumnCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream

/**
 * An abstract [Index] implementation that outlines the fundamental structure. Implementations of
 * [Index]es in Cottontail DB should inherit from this class.
 *
 * @see Index
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
abstract class AbstractIndex(final override val name: Name.IndexName, final override val parent: DefaultEntity) : Index {

    /** A [AbstractIndex] belongs to its [DefaultCatalogue]. */
    final override val catalogue: DefaultCatalogue = this.parent.catalogue

    /** The [DBOVersion] of this [AbstractIndex]. */
    override val version: DBOVersion = DBOVersion.V3_0

    /** Flag indicating if this [AbstractIndex] has been closed. */
    override val closed: Boolean
        get() = this.parent.closed

    /**
     * A [Tx] that affects this [AbstractIndex].
     */
    protected abstract inner class Tx(context: TransactionContext) : AbstractTx(context), IndexTx {

        /** Reference to the [AbstractIndex] */
        final override val dbo: AbstractIndex
            get() = this@AbstractIndex

        /** True, if the [AbstractIndex] backing this [Tx] supports incremental updates, and false otherwise. */
        override val supportsIncrementalUpdate: Boolean
            get() = this@AbstractIndex.supportsIncrementalUpdate

        /** True, if the [AbstractIndex] backing this [Tx] supports filtering an index-able range of the data. */
        override val supportsPartitioning: Boolean
            get() = this@AbstractIndex.supportsPartitioning

        /** The [ColumnDef] indexed by the [AbstractIndex] this [Tx] belongs to. */
        override val columns: Array<ColumnDef<*>> = IndexCatalogueEntry.read(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx)?.columns?.map {
                ColumnCatalogueEntry.read(it, this@AbstractIndex.catalogue, this.context.xodusTx)?.toColumnDef() ?: throw DatabaseException.DataCorruptionException("Failed to obtain columns for index ${this@AbstractIndex.name} because catalogue entry for column could not be read ${it}.")
            }?.toTypedArray() ?: throw DatabaseException.DataCorruptionException("Failed to obtain columns for index ${this@AbstractIndex.name}: Could not read catalogue entry for index.")

        /**
         * Flag indicating, if this [AbstractIndex] reflects all changes done to the [DefaultEntity] it belongs to.
         *
         * This object is accessed lazily, since it may change within the scope of a transactio.
         */
        override val state: IndexState
            get() = IndexCatalogueEntry.read(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx)?.state ?: throw DatabaseException.DataCorruptionException("Failed to obtain state for index ${this@AbstractIndex.name}: Could not read catalogue entry for index.")

        /**
         * Accessor for the [IndexConfig].
         *
         * This object is accessed lazily, since it may change within the scope of a transactio.
         */
        override val config: IndexConfig<*>
            get() {
                val entry = IndexCatalogueEntry.read(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this@AbstractIndex.name}.")
                return entry.config
            }

        /**
         * Obtains a global (non-exclusive) read-lock on [DefaultCatalogue].
         *
         * Prevents [DefaultCatalogue] from being closed while transaction is ongoing.
         */
        private val closeStamp: Long

        init {
            /** Checks if DBO is still open. */
            if (this.dbo.closed) throw TransactionException.DBOClosed(this.context.txId, this.dbo)
            this.closeStamp = this.dbo.catalogue.closeLock.readLock()
        }

        /**
         * Convenience method to update [IndexState] for this [AbstractHDIndex].
         *
         * @param state The new [IndexState].
         */
        protected fun updateState(state: IndexState, config: IndexConfig<*>? = null) {
            /* Obtain old entry and compare state. */
            val oldEntry = IndexCatalogueEntry.read(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to update state for index ${this@AbstractIndex.name}: Could not read catalogue entry for index.")
            if (oldEntry.state == state) return

            /* Copy entry... */
            val newEntry = if (config != null) {
                oldEntry.copy(state = state, config = config)
            } else {
                oldEntry.copy(state = state)
            }

            /* ... and write it to catalogue. */
            IndexCatalogueEntry.write(newEntry, this@AbstractIndex.catalogue, this.context.xodusTx)
        }

        /**
         * Called when a transaction finalizes. Releases the lock held on the [DefaultCatalogue].
         */
        override fun cleanup() {
            this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
        }
    }

    companion object {

        /**
         * Generates [Value] to be used for indexing. Combines multiple values in case of composite index.
         */
        fun combineValues(columns: Array<ColumnDef<*>>, values: Map<ColumnDef<*>, Value?>): Value? {

            if (columns.isEmpty()) {
                return null
            }
            if (columns.size == 1) {
                return values[columns[0]]
            }
            return combineValues(columns.map { values[it] })

        }

        /**
         * Generates [Value] to be used for indexing. Combines multiple values in case of composite index.
         */
        fun combineValues(columns: Array<ColumnDef<*>>, values: Record): Value? {

            if (columns.isEmpty()) {
                return null
            }
            if (columns.size == 1) {
                return values[columns[0]]
            }
            return combineValues(columns.map { values[it] })

        }
        private fun combineValues(values: List<Value?>): Value? {

            if (values.all { it == null }) {
                return null
            }

            val arrayStream = ByteArrayOutputStream()
            val out = DataOutputStream(arrayStream)

            for (value in values) {
                if (value == null) {
                    out.writeByte(0)
                } else {
                    when(value.type){
                        Types.Boolean -> out.writeBoolean((value as BooleanValue).value)
                        Types.ByteString -> out.write((value as ByteStringValue).value)
                        Types.Date -> out.writeLong((value as DateValue).value)
                        Types.Byte -> out.writeByte((value as ByteValue).value.toInt())
                        Types.Complex32 -> {
                            value as Complex32Value
                            out.writeFloat(value.data[0])
                            out.writeFloat(value.data[1])
                        }
                        Types.Complex64 -> {
                            value as Complex64Value
                            out.writeDouble(value.data[0])
                            out.writeDouble(value.data[1])
                        }
                        Types.Double -> out.writeDouble((value as DoubleValue).value)
                        Types.Float -> out.writeFloat((value as FloatValue).value)
                        Types.Int -> out.writeInt((value as IntValue).value)
                        Types.Long -> out.writeLong((value as LongValue).value)
                        Types.Short -> out.writeShort((value as ShortValue).value.toInt())
                        Types.String -> out.writeBytes((value as StringValue).value)
                        is Types.BooleanVector -> {
                            value as BooleanVectorValue
                            for (v in value.data) {
                                out.writeBoolean(v)
                            }
                        }
                        is Types.Complex32Vector -> {
                            value as Complex32VectorValue
                            for (v in value.data) {
                                out.writeFloat(v)
                            }
                        }
                        is Types.Complex64Vector -> {
                            value as Complex64Value
                            for (v in value.data) {
                                out.writeDouble(v)
                            }
                        }
                        is Types.DoubleVector -> {
                            value as DoubleVectorValue
                            for (v in value.data) {
                                out.writeDouble(v)
                            }
                        }
                        is Types.FloatVector -> {
                            value as FloatVectorValue
                            for (v in value.data) {
                                out.writeFloat(v)
                            }
                        }
                        is Types.IntVector -> {
                            value as IntVectorValue
                            for (v in value.data) {
                                out.writeInt(v)
                            }
                        }
                        is Types.LongVector -> {
                            value as LongVectorValue
                            for (v in value.data) {
                                out.writeLong(v)
                            }
                        }
                    }
                }
            }

            return ByteStringValue(arrayStream.toByteArray())
        }

    }

}
