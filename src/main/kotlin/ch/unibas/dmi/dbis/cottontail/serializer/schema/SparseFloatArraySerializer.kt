package ch.unibas.dmi.dbis.cottontail.serializer.schema

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

import java.io.IOException

class SparseFloatArraySerializer : Serializer<FloatArray> {

    @Throws(IOException::class)
    override fun serialize(out: DataOutput2, value: FloatArray) {
        out.writeShort(value.size.toShort().toInt())
        for (v in value) {
            if (v == 0.0f) {
                out.writeBoolean(false)
            } else {
                out.writeBoolean(true)
                out.writeFloat(v)
            }
        }
    }

    @Throws(IOException::class)
    override fun deserialize(input: DataInput2, available: Int): FloatArray {
        val ret = FloatArray(input.readShort().toInt())
        for (i in ret.indices) {
            if (input.readBoolean()) {
                ret[i] = input.readFloat()
            } else {
                ret[i] = 0.0f
            }
        }
        return ret
    }
}
