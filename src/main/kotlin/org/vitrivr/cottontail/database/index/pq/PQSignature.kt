package org.vitrivr.cottontail.database.index.pq

import org.mapdb.DataInput2
import org.mapdb.DataOutput2

/**
 * A fixed length signature used for a [PQIndex].
 *
 * TODO: Could be made more compact.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
data class PQSignature(val cells: IntArray) {
    companion object Serializer : org.mapdb.Serializer<PQSignature> {
        override fun serialize(out: DataOutput2, value: PQSignature) {
            out.packInt(value.cells.size)
            for (c in value.cells) {
                out.writeShort(c)
            }
        }

        override fun deserialize(input: DataInput2, available: Int) = PQSignature(IntArray(input.unpackInt()) { input.readUnsignedShort() })
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as PQSignature

        if (!this.cells.contentEquals(other.cells)) return false

        return true
    }

    override fun hashCode(): Int {
        return this.cells.contentHashCode()
    }
}