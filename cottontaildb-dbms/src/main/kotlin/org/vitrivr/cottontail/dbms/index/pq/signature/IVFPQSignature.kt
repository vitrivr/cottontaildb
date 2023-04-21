package org.vitrivr.cottontail.dbms.index.pq.signature

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.CompoundByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.dbms.index.pq.IVFPQIndex
import org.xerial.snappy.Snappy

/**
 * A IVF [PQSignature] as used by the [IVFPQIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class IVFPQSignature(val tupleId: TupleId, override val cells: ShortArray): PQSignature {
    /**
     * A Xodus binding to serialize and deserialize [IVFPQSignature].
     */
    companion object {
        /**
         * De-serializes a [IVFPQSignature] from a [ByteIterable].
         *
         * @param entry The [ByteIterable] to deserialize from.
         * @return Resulting [IVFPQSignature]
         */
        fun fromEntry(entry: ByteIterable): IVFPQSignature {
            val tupleId = LongBinding.entryToLong(entry)
            val cells = Snappy.uncompressShortArray(entry.subIterable(8, entry.length - 8).bytesUnsafe)
            return IVFPQSignature(tupleId, cells)
        }
    }

    /**
     * Converts this [SPQSignature] to a serializable entry.
     *
     * @return [ByteIterable]
     */
    fun toEntry(): ByteIterable {
        val tupleId = ArrayByteIterable(LongBinding.longToEntry(tupleId))
        val cells = ArrayByteIterable(Snappy.compress(this.cells))
        return CompoundByteIterable(arrayOf(tupleId, cells))
    }
}