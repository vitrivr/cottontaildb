package org.vitrivr.cottontail.dbms.sequence

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import java.util.concurrent.atomic.AtomicBoolean
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
        const val CATALOGUE_SEQUENCE_STORE_NAME: String = "org.vitrivr.cottontail.sequences"
    }
    /** A [DefaultSequence] belongs to the same [DefaultCatalogue] as the [DefaultSchema] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /**
     * Creates and returns a new [SequenceTx] for the given [SchemaTx].
     *
     * @param parent The [SchemaTx] to create the [SequenceTx] for.
     * @return New [DefaultSequence.Tx]
     */
    override fun newTx(parent: SchemaTx): DefaultSequence.Tx {
        require(parent is DefaultSchema.Tx) { "DefaultSequence.Tx can only be used with DefaultEntity.Tx" }
        return this.Tx(parent)
    }

    /**
     * A [Tx] that affects this [DefaultEntity].
     */
    inner class Tx(parent: DefaultSchema.Tx): SequenceTx, org.vitrivr.cottontail.dbms.general.Tx.WithCommitFinalization {
        /** Reference to the Cottontail DB [Transaction] object. */
        override val transaction: Transaction = parent.transaction

        /** Reference to the surrounding [DefaultEntity]. */
        override val dbo: DBO
            get() = this@DefaultSequence

        /** The catalogue-level Xodus [jetbrains.exodus.env.Transaction]. */
        internal val xodusTx: jetbrains.exodus.env.Transaction = parent.xodusTx

        /** Data [Store] used by this [DefaultSequence]. */
        private val store: Store = this.xodusTx.environment.openStore(CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.USE_EXISTING, xodusTx)

        /** The current [AtomicLong] of this [DefaultSequence]. Used to reduce I/O to disk during a transaction*/
        private val cache = AtomicLong(0L)

        /** Flag indicating, that there have been changes to this [DefaultSequence]. */
        private val dirty = AtomicBoolean(false)

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
        override fun next(): LongValue {
            this.dirty.compareAndSet(false, true)
            return LongValue(this.cache.incrementAndGet())
        }

        /**
         * Resets this [DefaultSequence].
         *
         * @return The [LongValue] value of this [DefaultSequence] before the reset.
         */
        override fun reset(): LongValue {
            this.dirty.compareAndSet(false, true)
            return LongValue(this.cache.getAndSet(0L))
        }

        /**
         * Returns the current value of this [DefaultSequence] without changing it.
         *
         * @return The current [LongValue] value of this [DefaultSequence].
         */
        override fun current(): LongValue = LongValue(this.cache.get())

        /**
         * Stores the current [cache] value of this [DefaultSequence].
         */
        override fun beforeCommit() {
            /* Load current sequence value from data store. */
            if (this.dirty.get()) {
                val raw = LongBinding.longToCompressedEntry(this.cache.get())
                this.store.put(xodusTx, NameBinding.Sequence.toEntry(this@DefaultSequence.name), raw)
            }
        }
    }
}
