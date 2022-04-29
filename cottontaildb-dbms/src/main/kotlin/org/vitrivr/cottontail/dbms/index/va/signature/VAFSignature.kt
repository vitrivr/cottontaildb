package org.vitrivr.cottontail.dbms.index.va.signature

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.xerial.snappy.Snappy

/**
 * A fixed length [VAFSignature] used in [VAFIndex] structures.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.1.0
 */
@JvmInline
value class VAFSignature(val cells: ByteArray): Comparable<VAFSignature> {

    companion object {
        val INVALID = VAFSignature(ByteArray(1) { -1 })
    }

    /**
     * A Xodus binding to serialize and deserialize [VAFSignature].
     */
    object Binding {
        fun entryToValue(entry: ByteIterable): VAFSignature = VAFSignature(Snappy.uncompress(entry.bytesUnsafe))
        fun valueToEntry(value: VAFSignature): ByteIterable {
            val compressed = Snappy.compress(value.cells)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }

    override fun compareTo(other: VAFSignature): Int {
        for ((i,b) in this.cells.withIndex()) {
            if (i >= other.cells.size) return Int.MIN_VALUE
            val comp = b.compareTo(other.cells[i])
            if (comp != 0) return comp
        }
        return 0
    }

    /**
     * Accessor for [VAFSignature].
     *
     * @param index The [index] to access.
     * @return [Int] value at the given [index].
     */
    operator fun get(index: Int): Byte = this.cells[index]

    /**
     * Checks if this [VAFSignature] is an invalid signature, which is used as a placeholder in cases no
     * valid [VAFSignature] could be obtained,
     *
     * @return True if [VAFSignature] is invalid, false otherwise.
     */
    fun invalid(): Boolean = this.cells[0] == INVALID[0]
}