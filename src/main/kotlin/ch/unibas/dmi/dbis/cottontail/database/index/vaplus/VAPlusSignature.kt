package ch.unibas.dmi.dbis.cottontail.database.index.vaplus

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import java.util.*

data class VAPlusSignature(val tupleId: Long, val signature: BitSet) {}

object VAPlusSignatureSerializer : Serializer<VAPlusSignature> {

    override fun serialize(out: DataOutput2, value: VAPlusSignature) {
        val signatureArray = value.signature.toLongArray()
        out.packLong(value.tupleId)
        out.packInt(signatureArray.size)
        out.packLongArray(signatureArray, 0, signatureArray.size)
    }

    override fun deserialize(input: DataInput2, available: Int): VAPlusSignature {
        val tupleId = input.readLong()
        val signature = LongArray(input.readInt())
        input.unpackLongArray(signature, 0, signature.size)
        return VAPlusSignature(tupleId, BitSet.valueOf(signature))
    }

}
