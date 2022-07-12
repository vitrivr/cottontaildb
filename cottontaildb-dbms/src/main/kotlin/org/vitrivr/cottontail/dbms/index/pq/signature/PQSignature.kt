package org.vitrivr.cottontail.dbms.index.pq.signature

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature
import org.xerial.snappy.Snappy

/**
 * A [PQSignature] as used by the [PQIndex]. Wraps an [IntArray].
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.1.0
 */
@JvmInline
value class PQSignature(val cells: IntArray): Comparable<PQSignature> {
    /**
     * A Xodus binding to serialize and deserialize [VAFSignature].
     */
    object Binding {
        fun entryToValue(entry: ByteIterable): PQSignature = PQSignature(Snappy.uncompressIntArray(entry.bytesUnsafe))
        fun valueToEntry(value: PQSignature): ByteIterable {
            val compressed = Snappy.compress(value.cells)
            return ArrayByteIterable(compressed, compressed.size)
        }
    }

    override fun compareTo(other: PQSignature): Int {
        for ((i,b) in this.cells.withIndex()) {
            if (i >= other.cells.size) return Int.MIN_VALUE
            val comp = b.compareTo(other.cells[i])
            if (comp != 0) return comp
        }
        return 0
    }
}