package org.vitrivr.cottontail.core.basics

import org.vitrivr.cottontail.core.database.TupleId

/**
 * A [Cursor] implementation for Cottontail DB to read data.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface Cursor<T>: CloseableIterator<T> {
    /**
     * Tries to move this [Cursor] to the next entry. Returns true on success and false otherwise.
     * If false is returned, the [Cursor] position should remain unchanged!
     *
     * @return True on success, false otherwise.
     */
    fun moveNext(): Boolean

    /**
     * Tries to move this [Cursor] to the previous entry. Returns true on success and false otherwise.
     * If false is returned, the [Cursor] position should remain unchanged! Not all [Cursor]s support this!
     *
     * @return True on success, false otherwise.
     */
    fun movePrevious(): Boolean = false

    /**
     * Tries to move this [Cursor] to the given [TupleId].
     *
     * Returns true on success, false otherwise. If false is returned, the [Cursor] position should remain unchanged!
     * Not all [Cursor]s support this!
     *
     * @param tupleId The [TupleId] to move to.
     * @return True on success, false otherwise.
     */
    fun moveTo(tupleId: TupleId): Boolean = false

    /**
     * Returns the [TupleId] this [Cursor] is currently pointing to.
     */
    fun key(): TupleId

    /**
     * Returns the value [T] this [Cursor] is currently pointing to.
     */
    fun value(): T

    /**
     * [Iterator] implementation: By default, a call to [next] simply returns [value]
     */
    override fun next(): T = this.value()

    /**
     * [Iterator] implementation: By default, a call to [hasNext] simply invokes [moveNext]
     */
    override fun hasNext(): Boolean = this.moveNext()
}