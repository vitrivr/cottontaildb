package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.FloatArrayValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * A [Serializer] for [FloatArrayValue]s that a fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedFloatVectorSerializer(val size: Int): Serializer<FloatArrayValue> {
    override fun serialize(out: DataOutput2, value: FloatArrayValue) {
        for (i in 0 until size) {
            out.writeFloat(value.value[i])
        }
    }
    override fun deserialize(input: DataInput2, available: Int): FloatArrayValue {
        val vector = FloatArray(size)
        for (i in 0 until size) {
            vector[i] = input.readFloat()
        }
        return FloatArrayValue(vector)
    }
}