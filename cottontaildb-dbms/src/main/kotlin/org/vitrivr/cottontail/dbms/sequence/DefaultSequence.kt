package org.vitrivr.cottontail.dbms.sequence

import it.unimi.dsi.fastutil.longs.Long2ObjectFunction
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import java.util.concurrent.atomic.AtomicLong

/**
 * The default [Sequence] implementation based on JetBrains Xodus.
 *
 * @see Sequence
 * @see SequenceTx
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
class DefaultSequence(override val name: Name.SequenceName, override val parent: DefaultSchema): Sequence {

    companion object {
        /** Name of the [Sequence] entry this [DefaultCatalogue]. */
        private const val CATALOGUE_SEQUENCE_STORE_NAME: String = "org.vitrivr.cottontail.sequences"

        /**
         * Returns the [Store] for [DefaultSequence] entries.
         *
         * @param transaction The Xodus [Transaction] to use.
         * @return [Store]
         */
        fun store(transaction: Transaction): Store = transaction.environment.openStore(CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction)
    }

    /** An internal cache of all ongoing [DefaultSchema.Tx]s for this [DefaultSchema]. */
    private val transactions = Long2ObjectOpenHashMap<DefaultSequence.Tx>()

    /** A [DefaultSequence] belongs to the same [DefaultCatalogue] as the [DefaultSchema] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /**
     * Creates and returns a new [DefaultSequence.Tx] for the given [QueryContext].
     *
     * @param parent The parent [SchemaTx] to create the [DefaultSequence.Tx] for.
     * @return New or cached [DefaultSequence.Tx].
     */
    override fun newTx(parent: SchemaTx): SequenceTx {
        require(parent is DefaultSchema.Tx) { "A DefaultSequence can only be accessed with a DefaultSchema.Tx!" }
        return this.transactions.computeIfAbsent(parent.context.txn.transactionId, Long2ObjectFunction {
            val subTransaction = Tx(parent)
            parent.context.txn.registerSubtransaction(subTransaction)
            subTransaction
        })
    }

    /**
     * A [Tx] that affects this [DefaultEntity].
     */
    inner class Tx(override val parent: DefaultSchema.Tx): SequenceTx, org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction.WithCommit {

        /** The Xodus transaction object used by this [DefaultSchema]. */
        private val xodusTx = this.parent.xodusTx

        /** Reference to the surrounding [DefaultEntity]. */
        override val dbo: DBO
            get() = this@DefaultSequence

        /** The current [AtomicLong] of this [DefaultSequence]. Used to reduce I/O to disk during a transaction*/
        private val cache = AtomicLong(0L)

        /** Data [Store] used by this [DefaultSequence]. */
        private val store: Store = store(this.xodusTx)

        init {
            /* Load current sequence value from data store. */
            val raw = this.store.get(this.xodusTx, NameBinding.Sequence.toEntry(this@DefaultSequence.name))
            if (raw != null) {
                this.cache.set(LongBinding.compressedEntryToLong(raw))
            }
        }

        /**
         * Returns the next value of this [DefaultSequence].
         *
         * @return The next [LongValue] value in this [DefaultSequence].
         */
        @Synchronized
        override fun next(): LongValue = LongValue(this.cache.incrementAndGet())

        /**
         * Returns the current value of this [DefaultSequence] without changing it.
         *
         * @return The current [LongValue] value of this [DefaultSequence].
         */
        @Synchronized
        override fun current(): LongValue = LongValue(this.cache.get())

        /**
         * Resets this [DefaultSequence].
         *
         * @return The [LongValue] value of this [DefaultSequence] before the reset.
         */
        @Synchronized
        override fun reset(): LongValue = LongValue(this.cache.getAndSet(0L))

        /**
         * Stores the current [cache] value of this [DefaultSequence].
         */
        @Synchronized
        override fun prepareCommit(): Boolean {
            val raw = LongBinding.longToCompressedEntry(this.cache.get())
            return this.store.put(this.xodusTx, NameBinding.Sequence.toEntry(this@DefaultSequence.name), raw)
        }

        /**
         *  This method has no logic.
         */
        @Synchronized
        override fun commit() {
            this@DefaultSequence.transactions.remove(this.context.txn.transactionId)
        }
    }
}
