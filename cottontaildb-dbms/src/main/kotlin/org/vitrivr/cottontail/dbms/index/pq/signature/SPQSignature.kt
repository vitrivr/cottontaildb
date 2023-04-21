package org.vitrivr.cottontail.dbms.index.pq.signature

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.xerial.snappy.Snappy

/**
 * A simple [PQSignature] as used by the [PQIndex].
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.1.0
 */
@JvmInline
value class SPQSignature(override val cells: ShortArray): PQSignature {
    /**
     * A Xodus binding to serialize and deserialize [SPQSignature].
     */
    companion object {
        /**
         * De-serializes a [SPQSignature] from a [ByteIterable].
         *
         * @param entry The [ByteIterable] to deserialize from.
         * @return Resulting [SPQSignature]
         */
        fun fromEntry(entry: ByteIterable): SPQSignature = SPQSignature(Snappy.uncompressShortArray(entry.bytesUnsafe))
    }

    /**
     * Converts this [SPQSignature] to a serializable entry.
     *
     * @return [ByteIterable]
     */
    fun toEntry(): ByteIterable {
        val compressed = Snappy.compress(this.cells)
        return ArrayByteIterable(compressed, compressed.size)
    }
}