package org.vitrivr.cottontail.dbms.index.lsh

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.index.lsh.signature.LSHSignature

/**
 * A [Cursor] implementation for the [LSHCursor].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class LSHCursor(index: LSHIndex.Tx, context: BindingContext, val predicate: ProximityPredicate): Cursor<Tuple> {
    /** Sub transaction for this [Cursor]. */
    private val xodusTx = index.xodusTx.readonlySnapshot

    /** The store containing the [LSHCursor] entries. */
    private val store: Store = this.xodusTx.environment.openStore(index.dbo.name.storeName(), StoreConfig.WITH_DUPLICATES_WITH_PREFIXING, this.xodusTx)

    /** Cursor backing this [LSHCursor]. */
    private val cursor = this.store.openCursor(this.xodusTx)

    /* Performs some sanity checks. */
    init {
        val config = index.config as LSHIndexConfig
        if (this.predicate.columns.first() != index.columns[0] || this.predicate.distance.name != config.distance) {
            throw QueryException.UnsupportedPredicateException("Index '${index.dbo.name}' (lsh-index) does not support the provided predicate.")
        }

        /* Assure correctness of query vector. */
        with(context) {
            with(MissingTuple) {
                val value = this@LSHCursor.predicate.query.getValue()
                check(value is VectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }

                /* Obtain LSH signature of query and set signature. */
                val signature = config.generator?.generate(value)
                check(signature != null) { "Failed to generate signature for query vector." }
                this@LSHCursor.cursor.getSearchKey(LSHSignature.Binding.objectToEntry(signature))
            }
        }
    }

    /**
     * Moves this [Cursor] by one entry.
     *
     * Returns true upon success and false if there is no entry left.
     */
    override fun moveNext(): Boolean = this.cursor.nextDup

    /**
     * Returns the next [Tuple] value.
     *
     * @return Next [Tuple]
     */
    override fun next(): Tuple = this.value()

    /**
     * Returns the next [Tuple] value.
     *
     * @return Next [Tuple]
     */
    override fun value(): Tuple = StandaloneTuple(this.key(), emptyArray(), emptyArray())

    /**
     * Returns the next [TupleId].
     *
     * @return Next [TupleId]
     */
    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.value)

    /**
     * Closes the internal Xodus [Cursor] and finalizes the sub transaction.
     */
    override fun close() {
        this.cursor.close()
        this.xodusTx.commit()
    }
}