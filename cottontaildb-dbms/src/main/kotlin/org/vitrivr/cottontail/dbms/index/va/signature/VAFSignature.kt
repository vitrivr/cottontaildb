package org.vitrivr.cottontail.dbms.index.va.signature

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.util.LightOutputStream
import org.xerial.snappy.Snappy


/**
 * A fixed length [VAFSignature] used for vector approximation.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.1.0
 */
@JvmInline
value class VAFSignature(val cells: IntArray): Comparable<VAFSignature> {
    /**
     * Accessor for [VAFSignature].
     *
     * @param index The [index] to access.
     * @return [Int] value at the given [index].
     */
    operator fun get(index: Int): Int = this.cells[index]

    companion object {
        fun entryToValue(entry: ByteIterable): VAFSignature = VAFSignature(Snappy.uncompressIntArray(entry.bytesUnsafe))
        fun valueToEntry(signature: VAFSignature): ByteIterable {
            val stream = LightOutputStream()
            val compressed = Snappy.compress(signature.cells)
            stream.write(compressed)
            return stream.asArrayByteIterable()
        }
    }

    override fun compareTo(other: VAFSignature): Int {
        for ((i,b) in this.cells.withIndex()) {
            if (i >= other.cells.size) return Int.MIN_VALUE
            val comp = b.compareTo(other.cells[i])
            if (comp != 0)  return comp
        }
        return 0
    }
}