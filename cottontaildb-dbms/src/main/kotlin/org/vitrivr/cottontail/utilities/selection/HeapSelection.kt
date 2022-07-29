package org.vitrivr.cottontail.utilities.selection

import it.unimi.dsi.fastutil.objects.ObjectHeaps
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.util.concurrent.locks.StampedLock

/**
 * A data structure used for heap based sorting.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Suppress("UNCHECKED_CAST")
class HeapSelection<T>(private val heap: Array<T?>, val comparator: Comparator<T>) {

    constructor(k: Int, comparator: Comparator<T>) : this(arrayOfNulls<Any?>(k) as Array<T?>, comparator)

    /** A lock that mediates access to this [HeapSelection]. */
    private val lock = StampedLock()

    /** The value k is determined by the heap size.*/
    val k: Int
        get() = this.heap.size

    /** Number of items that have been added to this [HeapSelection] so far. */
    @Volatile
    var added: Int = 0
        private set

    /** Returns the size of this [HeapSelection], i.e., the number of items contained in the heap. */
    @Volatile
    var size: Int = 0
        private set

    init {
        try {
            ObjectHeaps.makeHeap(this.heap, this.k, this.comparator as java.util.Comparator<in T?>)
            this.size = this.k
            this.added = this.k
        } catch (e: NullPointerException) {
            /* No op. */
        }
    }

    /**
     * Adds a new element to this [HeapSelection].
     *
     * @param element The element to add to this [HeapSelection].
     * @return The smallest element in the heap.
     */
    fun enqueue(element: T): T = this.lock.write {
        if (this.size < this.k) {
            this.heap[this.size++] = element
            ObjectHeaps.upHeap(this.heap, this.size, this.size - 1, this.comparator)
        } else if (this.comparator.compare(element, this.heap[0]) < 0) {
            this.heap[0] = element
            ObjectHeaps.downHeap(this.heap, this.k, 0, this.comparator as java.util.Comparator<in T?>)
        }
        this.added += 1
        this.heap[0]!!
    }

    /**
     * Returns the i-*th* value seen by this [HeapSelection].
     *
     * @return The i-th smallest value retained by this [HeapSelection]
     */
    fun dequeue(): T = this.lock.write {
        if (this.size == 0) throw NoSuchElementException("HeapSelect is empty!")
        val result = this.heap[0]
        this.heap[0] = this.heap[--this.size]
        this.heap[this.size] = null
        if (this.size != 0) ObjectHeaps.downHeap(this.heap, this.size, 0, this.comparator as java.util.Comparator<in T?>)
        return result!!
    }

    /**
     * Returns true if this [HeapSelection] is empty and false otherwise.
     *
     * @return True if this [HeapSelection] is empty and false otherwise
     */
    fun isEmpty(): Boolean = this.lock.read { this.size == 0 }
}