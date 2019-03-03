package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.DoubleArrayValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * A [Serializer] for [DoubleArrayValue]s that a fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedDoubleVectorSerializer(val size: Int): Serializer<DoubleArrayValue> {
    override fun serialize(out: DataOutput2, value: DoubleArrayValue) {
        for (i in 0 until size) {
            out.writeDouble(value.value[i])
        }
    }
    override fun deserialize(input: DataInput2, available: Int): DoubleArrayValue {
        val vector = DoubleArray(size)
        for (i in 0 until size) {
            vector[i] = input.readDouble()
        }
        return DoubleArrayValue(vector)
    }
}