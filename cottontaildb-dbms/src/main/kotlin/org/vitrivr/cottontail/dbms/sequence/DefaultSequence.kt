package org.vitrivr.cottontail.dbms.sequence

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
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import java.util.concurrent.atomic.AtomicLong

/**
 * The default [Sequence] implementation based on JetBrains Xodus.
 *
 * @see Sequence
 * @see SequenceTx
 *
 * @author Ralph Gasser
 * @version 3.1.0
 */
class DefaultSequence(override val name: Name.SequenceName, override val parent: DefaultSchema): Sequence {
    companion object {
        /** Name of the [Sequence] entry this [DefaultCatalogue]. */
        private const val CATALOGUE_SEQUENCE_STORE_NAME: String = "org.vitrivr.cottontail.sequences"

        /**
         * Initializes the store used to store [Sequence]s in Cottontail DB.
         *
         * @param catalogue [DefaultCatalogue] to initialize the [Sequence] store for.
         * @param transaction The Xodus [Transaction] to use.
         */
        fun init(catalogue: DefaultCatalogue, transaction: Transaction) {
            catalogue.transactionManager.environment.openStore(CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create store for sequence catalogue.")
        }

        /**
         * Returns the [Store] for [DefaultSequence] entries.
         *
         * @param catalogue [DefaultCatalogue] to retrieve [DefaultSequence] from.
         * @param transaction The Xodus [Transaction] to use.
         * @return [Store]
         */
        fun store(catalogue: DefaultCatalogue, transaction: Transaction): Store {
            return catalogue.transactionManager.environment.openStore(CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Data store for sequences is missing.")
        }
    }

    /** A [DefaultSequence] belongs to the same [DefaultCatalogue] as the [DefaultSchema] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /**
     * Creates and returns a new [DefaultSequence.Tx] for the given [QueryContext].
     *
     * @param context The [QueryContext] to create the [DefaultSequence.Tx] for.
     * @return New or cached [DefaultSequence.Tx].
     */
    override fun newTx(context: QueryContext): SequenceTx
        = context.txn.getCachedTxForDBO(this) ?: this.Tx(context)

    /**
     * A [Tx] that affects this [DefaultEntity].
     */
    inner class Tx(context: QueryContext): AbstractTx(context), SequenceTx, org.vitrivr.cottontail.dbms.general.Tx.WithCommitFinalization {

        /** Reference to the surrounding [DefaultEntity]. */
        override val dbo: DBO
            get() = this@DefaultSequence

        /** The current [AtomicLong] of this [DefaultSequence]. Used to reduce I/O to disk during a transaction*/
        private val cache = AtomicLong(0L)

        /** Data [Store] used by this [DefaultSequence]. */
        private val store: Store = store(this@DefaultSequence.catalogue, this.context.txn.xodusTx)

        init {
            /* Load current sequence value from data store. */
            val raw = this.store.get(this.context.txn.xodusTx, NameBinding.Sequence.toEntry(this@DefaultSequence.name))
            if (raw != null) {
                this.cache.set(LongBinding.compressedEntryToLong(raw))
            }
        }

        /**
         * Returns the next value of this [DefaultSequence].
         *
         * @return The next [LongValue] value in this [DefaultSequence].
         */
        override fun next(): LongValue = LongValue(this.cache.incrementAndGet())

        /**
         * Returns the current value of this [DefaultSequence] without changing it.
         *
         * @return The current [LongValue] value of this [DefaultSequence].
         */
        override fun current(): LongValue = LongValue(this.cache.get())

        /**
         * Resets this [DefaultSequence].
         *
         * @return The [LongValue] value of this [DefaultSequence] before the reset.
         */
        override fun reset(): LongValue = LongValue(this.cache.getAndSet(0L))

        /**
         * Stores the current [cache] value of this [DefaultSequence].
         */
        override fun beforeCommit() {
            /* Load current sequence value from data store. */
            val raw = LongBinding.longToCompressedEntry(this.cache.get())
            this.store.put(this.context.txn.xodusTx, NameBinding.Sequence.toEntry(this@DefaultSequence.name), raw)
        }
    }
}
