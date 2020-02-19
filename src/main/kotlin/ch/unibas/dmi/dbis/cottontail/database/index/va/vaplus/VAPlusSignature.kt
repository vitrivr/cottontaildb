package ch.unibas.dmi.dbis.cottontail.database.index.va.vaplus

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

data class VAPlusSignature(val tupleId: Long, val signature: IntArray) {}

object VAPlusSignatureSerializer : Serializer<VAPlusSignature> {

    override fun serialize(out: DataOutput2, value: VAPlusSignature) {
        out.packLong(value.tupleId)
        out.packInt(value.signature.size)
        for (signature in value.signature) {
            out.writeInt(signature)
        }
    }

    override fun deserialize(input: DataInput2, available: Int): VAPlusSignature {
        val tupleId = input.readLong()
        val signature = IntArray(input.readInt()) {
            input.readInt()
        }
        return VAPlusSignature(tupleId, signature)
    }

}
