package ch.unibas.dmi.dbis.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * A [Serializer] for [FloatArray]s that a fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedFloatVectorSerializer(val size: Int): Serializer<FloatArray> {
    override fun serialize(out: DataOutput2, value: FloatArray) {
        for (i in 0 until size) {
            out.writeFloat(value[i])
        }
    }
    override fun deserialize(input: DataInput2, available: Int): FloatArray {
        val vector = FloatArray(size)
        for (i in 0 until size) {
            vector[i] = input.readFloat()
        }
        return vector
    }
}