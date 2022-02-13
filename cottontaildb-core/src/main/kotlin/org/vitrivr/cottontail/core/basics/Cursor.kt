package org.vitrivr.cottontail.core.basics

import org.vitrivr.cottontail.core.database.TupleId

/**
 * A [Cursor] implementation for Cottontail DB to read data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Cursor<T>: AutoCloseable, Iterator<T> {
    /**
     * Tries to move this [Cursor]. Returns true on success and false otherwise.
     */
    fun moveNext(): Boolean

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