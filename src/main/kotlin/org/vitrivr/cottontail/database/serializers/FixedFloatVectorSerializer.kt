package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.values.FloatVectorValue

/**
 * A [Serializer] for [FloatVectorValue]s that a fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedFloatVectorSerializer(val size: Int) : Serializer<FloatVectorValue> {
    override fun serialize(out: DataOutput2, value: FloatVectorValue) {
        for (i in 0 until size) {
            out.writeFloat(value.value[i])
        }
    }

    override fun deserialize(input: DataInput2, available: Int): FloatVectorValue {
        val vector = FloatArray(size) { input.readFloat() }
        return FloatVectorValue(vector)
    }
}