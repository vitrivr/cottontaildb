package org.vitrivr.cottontail.dbms.index.pq.signature

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.xerial.snappy.Snappy

/**
 * A [PQSignature] as used by the [PQIndex]. Wraps an [IntArray].
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.1.0
 */
@JvmInline
value class PQSignature(val cells: ShortArray) {
    /**
     * A Xodus binding to serialize and deserialize [PQSignature].
     */
    companion object {
        /**
         * De-serializes a [PQSignature] from a [ByteIterable].
         *
         * @param entry The [ByteIterable] to deserialize from.
         * @return Resulting [PQSignature]
         */
        fun fromEntry(entry: ByteIterable): PQSignature = PQSignature(Snappy.uncompressShortArray(entry.bytesUnsafe))
    }

    /**
     * Converts this [PQSignature] to a serializable entry.
     *
     * @return [ByteIterable]
     */
    fun toEntry(): ByteIterable {
        val compressed = Snappy.compress(this.cells)
        return ArrayByteIterable(compressed, compressed.size)
    }
}