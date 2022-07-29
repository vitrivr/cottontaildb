package org.vitrivr.cottontail.utilities.selection

import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.lang.Integer.min
import java.util.NoSuchElementException
import java.util.concurrent.locks.StampedLock

/**
 * A data structure used for heap based sorting.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Suppress("UNCHECKED_CAST")
class HeapSelection<T>(val k: Int, val comparator: Comparator<T>) : Iterable<T> {

    private val heap: Array<T?> = arrayOfNulls<Any?>(k) as Array<T?>

    /** A lock that mediates access to this [HeapSelection]. */
    private val lock = StampedLock()

    /** Indicates whether this [MinHeapSelection] is currently sorted or not. */
    @Volatile
    var sorted: Boolean = true
        private set

    /** Number of items that have been added to this [HeapSelection] so far. */
    @Volatile
    var added: Long = 0
        private set

    /** Returns the size of this [HeapSelection], i.e., the number of items contained in the heap. */
    @Volatile
    var size: Int = 0
        private set

    /**
     * Adds a new element to this [HeapSelection].
     *
     * @param element The element to add to this [HeapSelection].
     */
    fun offer(element: T): T = this.lock.write {
        if (this.size < this.heap.size) {
            this.sorted = false
            this.heap[this.size++] = element
            if (this.size == this.k) {
                heapify()
            }
        } else {
            if (this.comparator.compare(element, this.heap[0]) < 0) {
                this.heap[0] = element
                siftDown(0, this.heap.size - 1)
            }
        }
        this.added += 1
        this.heap[0]!!
    }

    /**
     * Returns the largest (i.e. i == k-1) value retained by this [HeapSelection].
     *
     * @return Largest value seen so far.
     */
    fun peek(): T? = this.lock.read { this.heap.firstOrNull() }

    /**
     * Returns the i-*th* retained value seen  by this [MinHeapSelection]. i = 0 returns the
     * smallest value seen, i = 1 the second smallest, ..., i = k-1 the largest value tracked.
     * Also, i must be less than the number of previous assimilated.
     *
     * @param i The index of the value to return
     * @return The i-th smallest value retained by this [MinHeapSelection]
     */
    operator fun get(i: Int): T {
        if (i == this.heap.size - 1) {
            this.lock.read { return this.heap[0] ?: throw NoSuchElementException("") }
        }
        return if (!this.sorted) {
            this.lock.write {
                val maxIdx = this.heap.size - 1
                this.sort()
                this.heap[maxIdx - i] ?: throw NoSuchElementException("")
            }
        } else {
            this.lock.read {
                val maxIdx = this.heap.size - 1
                this.heap[maxIdx - i]?: throw NoSuchElementException("")
            }
        }
    }


    /**
     * Sorts the heap so that the smallest values move to the top of the [HeapSelection].
     */
    private fun sort() {
        val n = min(this.heap.size, this.size)
        var inc = 1
        do {
            inc *= 3
            inc++
        } while (inc <= n)

        do {
            inc /= 3
            for (i in inc until n) {
                val v = this.heap[i]
                var j = i
                while (this.comparator.compare(this.heap[j - inc], v) < 0) {
                    this.heap[j] = this.heap[j - inc]
                    j -= inc
                    if (j < inc) {
                        break
                    }
                }
                this.heap[j] = v
            }
        } while (inc > 1)
        this.sorted = true
    }

    /**
     *
     */
    private fun siftDown(i: Int, n: Int) {
        var k = i
        while (2 * k <= n) {
            var j = 2 * k
            if (j < n && this.comparator.compare(this.heap[j], this.heap[j + 1]) < 0) {
                j++
            }
            if (this.comparator.compare(this.heap[k], this.heap[j]) >= 0) break

            /* Swap elements. */
            val a = this.heap[k]
            this.heap[k] = this.heap[j]
            this.heap[j] = a
            k = j
        }
    }

    /**
     * Heapifies this [HeapSelection].
     */
    private fun heapify() {
        val n = this.heap.size
        for (i in Math.floorDiv(n, 2) downTo 0) {
            siftDown(i, n - 1)
        }
    }

    /**
     * Returns true if this [HeapSelection] is empty and false otherwise.
     *
     * @return True if this [HeapSelection] is empty and false otherwise
     */
    fun isEmpty(): Boolean = this.lock.read { this.size == 0 }

    /**
     *
     */
    override fun iterator(): Iterator<T> = object : Iterator<T> {
        private var index: Int = 0
        override fun hasNext(): Boolean = this.index < this@HeapSelection.size - 1
        override fun next(): T = this@HeapSelection.heap[this.index++] ?: throw NoSuchElementException()
    }
}