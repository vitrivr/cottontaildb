package org.vitrivr.cottontail.utilities.selection

import java.lang.Integer.min

/**
 * A data structure that can be used to retain the [k] smallest values with respect to a given [Comparator].
 *
 * This data structure is not thread safe and concurrent access must therefore be synchronised!
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
@Suppress("UNCHECKED_CAST")
class HeapSelection<T>(val k: Int, val comparator: Comparator<T>) : Iterable<T> {
    /** The [Array] backing this [HeapSelection].   */
    private val heap: Array<T?> = arrayOfNulls<Any?>(this.k) as Array<T?>

    /** Indicates whether this [MinHeapSelection] is currently sorted or not. */
    var sorted: Boolean = true
        private set

    /** Number of items that have been added to this [HeapSelection] so far. */
    var added: Long = 0
        private set

    /** Returns the size of this [HeapSelection], i.e., the number of items contained in the heap. */
    var size: Int = 0
        private set

    /**
     * Adds a new element to this [HeapSelection].
     *
     * @param element The element to add to this [HeapSelection].
     */
    fun offer(element: T): T {
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
        return this.heap[0]!!
    }

    /**
     * Returns the largest (i.e. i == k-1) value retained by this [HeapSelection].
     *
     * @return Largest value seen so far.
     */
    fun peek(): T? = this.heap.firstOrNull()

    /**
     * Returns the i-*th* value held by this [HeapSelection]. i = 0 returns the smallest value seen,
     * i = 1 the second smallest, ..., i = k-1 the largest value tracked.
     *
     * @param i The index of the value to return
     * @return The i-th smallest value retained by this [MinHeapSelection]
     */
    operator fun get(i: Int): T {
        val maxId =  this.heap.size - 1
        if (i == maxId) {
            return this.heap[0] ?: throw NoSuchElementException("Element at index $i does not exist in HeapSelection.")
        } else if (!this.sorted) {
            this.sort()
        }
        return this.heap[maxId - i] ?: throw NoSuchElementException("Element at index $i does not exist in HeapSelection.")
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
        for (i in Math.floorDiv(this.heap.size, 2) downTo 0) {
            siftDown(i, this.heap.size - 1)
        }
    }

    /**
     * Returns true if this [HeapSelection] is empty and false otherwise.
     *
     * @return True if this [HeapSelection] is empty and false otherwise
     */
    fun isEmpty(): Boolean = this.size == 0

    /**
     * Creates and returns an [Iterator] for this [HeapSelection].
     *
     * Creating an [Iterator] causes the [HeapSelection] to be sorted. If elements are added to the
     * [HeapSelection] after creating the [Iterator], the sort order may become incorrect and a call
     * next will cause a [ConcurrentModificationException] to be thrown.
     *
     * @return [Iterator] for this [HeapSelection].
     */
    override fun iterator(): Iterator<T> {
        if (!this.sorted) this.sort()
        return object : Iterator<T> {
            private val expectedAdded: Long = this@HeapSelection.added
            private var index: Int = this@HeapSelection.size - 1
            override fun hasNext(): Boolean = this.index > 0
            override fun next(): T {
                if(this.expectedAdded != this@HeapSelection.added) throw ConcurrentModificationException("HeapSelection was modified while iterating over it.")
                return this@HeapSelection.heap[this.index--] ?: throw NoSuchElementException("Iterator for HeapSelection has no more elements left.")
            }
        }
    }
}