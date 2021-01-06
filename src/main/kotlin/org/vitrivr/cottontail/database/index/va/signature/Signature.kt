package org.vitrivr.cottontail.database.index.va.signature

import org.mapdb.DataInput2
import org.mapdb.DataOutput2

/**
 * A fixed length [Signature] used for vector approximation.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.0
 */
data class Signature(val tupleId: Long, val cells: IntArray) {
    companion object Serializer : org.mapdb.Serializer<Signature> {
        override fun serialize(out: DataOutput2, value: Signature) {
            org.mapdb.Serializer.LONG_DELTA.serialize(out, value.tupleId)
            org.mapdb.Serializer.INT_ARRAY.serialize(out, value.cells)
        }

        override fun deserialize(input: DataInput2, available: Int) = Signature(
                org.mapdb.Serializer.LONG_DELTA.deserialize(input, available),
                org.mapdb.Serializer.INT_ARRAY.deserialize(input, available)
        )
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Signature

        if (tupleId != other.tupleId) return false
        if (!cells.contentEquals(other.cells)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = tupleId.hashCode()
        result = 31 * result + cells.contentHashCode()
        return result
    }
}