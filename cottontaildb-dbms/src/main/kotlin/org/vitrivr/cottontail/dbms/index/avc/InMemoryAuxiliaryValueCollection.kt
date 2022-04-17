package org.vitrivr.cottontail.dbms.index.basics.avc

import it.unimi.dsi.fastutil.longs.LongLinkedOpenHashSet
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.vitrivr.cottontail.core.database.TupleId
import java.util.*

/**
 * An [AuxiliaryValueCollection] implementation that stores all the value in memory (i.e., provides no persistence).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class InMemoryAuxiliaryValueCollection: AuxiliaryValueCollection {

    /** Set of all INSERTS applied to this [InMemoryAuxiliaryValueCollection]. */
    private val _insertSet = LongLinkedOpenHashSet()
    override val insertSet: SortedSet<TupleId>
        get() = Collections.unmodifiableSortedSet(this._insertSet)

    /** Set of all UPDATES applied to this [InMemoryAuxiliaryValueCollection]. */
    private val _updateSet  = LongOpenHashSet()
    override val updateSet: Set<TupleId>
        get() = Collections.unmodifiableSet(this._updateSet)

    /** Map of all DELETES applied to this [InMemoryAuxiliaryValueCollection]. */
    private val _deleteSet = LongOpenHashSet()
    override val deleteSet: Set<TupleId>
        get() = Collections.unmodifiableSet(this._deleteSet)

    /**
     * Applies an INSERT operation for the given [TupleId] and [Value].
     *
     * @param tupleId The [TupleId] that has been inserted.
     */
    override fun applyInsert(tupleId: TupleId) {
        require(!this.deleteSet.contains(tupleId)) { "TupleId $tupleId has been logged for DELETE and cannot be logged INSERT anymore."}
        require(!this.updateSet.contains(tupleId)) { "TupleId $tupleId has been logged for UPDATE and cannot be logged for INSERT again."}
        require(!this.insertSet.contains(tupleId)) { "TupleId $tupleId has been logged for INSERT and cannot be logged for INSERT again."}
        this._insertSet.add(tupleId)
        Unit
    }

    /**
     * Applies an UPDATE operation for the given [TupleId] and [Value].
     *
     * @param tupleId The [TupleId] that has been updated.
     */
    override fun applyUpdate(tupleId: TupleId) {
        require(!this.deleteSet.contains(tupleId)) { "TupleId $tupleId has been logged for DELETE and cannot be logged for UPDATE again."}
        if (!this.insertSet.contains(tupleId)) {
            this._updateSet.add(tupleId) /* If INSERT has not been registered by this AVC, it will be part of the UPDATE set. */
        }
    }

    /**
     * Applies an DELETE operation for the given [TupleId] and [Value].
     *
     * @param tupleId The [TupleId] that has been deleted.
     */
    override fun applyDelete(tupleId: TupleId) {
        if (!this.deleteSet.contains(tupleId)) {
            this._insertSet.remove(tupleId) /*  If INSERT for this tupleId has been registered by this AVC, that id is removed from INSERT set. */
            this._updateSet.remove(tupleId) /*  If UPDATE for this tupleId has been registered by this AVC, that id is removed from UPDATE set. */
            this._deleteSet.add(tupleId)
        }
    }
}