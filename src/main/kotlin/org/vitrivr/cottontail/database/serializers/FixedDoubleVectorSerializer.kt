package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.values.DoubleVectorValue

/**
 * A [Serializer] for [DoubleVectorValue]s that a fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedDoubleVectorSerializer(val size: Int) : Serializer<DoubleVectorValue> {
    override fun serialize(out: DataOutput2, value: DoubleVectorValue) {
        for (i in 0 until size) {
            out.writeDouble(value.value[i])
        }
    }

    override fun deserialize(input: DataInput2, available: Int): DoubleVectorValue {
        val vector = DoubleArray(size)
        for (i in 0 until size) {
            vector[i] = input.readDouble()
        }
        return DoubleVectorValue(vector)
    }
}