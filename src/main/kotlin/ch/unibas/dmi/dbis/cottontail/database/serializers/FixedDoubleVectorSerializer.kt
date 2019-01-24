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
class FixedDoubleVectorSerializer(val size: Int): Serializer<DoubleArray> {
    override fun serialize(out: DataOutput2, value: DoubleArray) {
        for (i in 0 until size) {
            out.writeDouble(value[i])
        }
    }
    override fun deserialize(input: DataInput2, available: Int): DoubleArray {
        val vector = DoubleArray(size)
        for (i in 0 until size) {
            vector[i] = input.readDouble()
        }
        return vector
    }
}