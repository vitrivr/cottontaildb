package org.vitrivr.cottontail.dbms.index.va.signature

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.dbms.index.va.VAFIndex

/**
 * A fixed length [VAFSignature] used in [VAFIndex] structures.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.1.0
 */
@JvmInline
value class VAFSignature(val cells: ByteArray) {

    companion object {
        /**
         * De-serializes a [VAFSignature] from a [ByteIterable].
         *
         * @param entry The [ByteIterable] to deserialize from.
         * @return Resulting [VAFSignature]
         */
        fun fromEntry(entry: ByteIterable): VAFSignature = VAFSignature(entry.bytesUnsafe)
    }

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
     * Converts this [VAFSignature] to an [ArrayByteIterable].
     *
     * @return The [ArrayByteIterable] representation of this [VAFSignature].
     */
    fun toEntry(): ByteIterable
        = ArrayByteIterable(this.cells, this.cells.size)
}