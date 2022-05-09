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
value class VAFSignature(val cells: ByteArray) {

    companion object {

        /** The invalid signature is a 1d-byte array. */
        val INVALID = VAFSignature(ByteArray(1){ Byte.MIN_VALUE })

        /**
         * De-serializes a [VAFSignature] from a [ByteIterable].
         *
         * @param entry The [ByteIterable] to deserialize from.
         * @return Resulting [VAFSignature]
         */
        fun fromEntry(entry: ByteIterable): VAFSignature = VAFSignature(Snappy.uncompress(entry.bytesUnsafe))
    }

    /**
     * Checks if this [VAFSignature] is invalid.
     *
     * @return True, if this [VAFSignature] is invalid, false otherwise.
     */
    fun isInvalid(): Boolean
        = this.cells.size == 1 && this.cells[0] == Byte.MIN_VALUE

    /**
     * Accessor for [VAFSignature].
     *
     * @param index The [index] to access.
     * @return [Int] value at the given [index].
     */
    operator fun get(index: Int): Int = this.cells[index].toInt()

    /**
     * The size of this [VAFSignature]
     */
    fun size(): Int = this.cells.size

    /**
     *
     */
    fun toEntry(): ByteIterable {
        val compressed = Snappy.compress(this.cells)
        return ArrayByteIterable(compressed, compressed.size)
    }
}