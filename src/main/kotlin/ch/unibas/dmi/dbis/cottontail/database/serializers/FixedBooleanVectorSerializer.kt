package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.BooleanArrayValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer


class FixedBooleanVectorSerializer(val size: Int): Serializer<BooleanArrayValue> {
    override fun serialize(out: DataOutput2, value: BooleanArrayValue) {
        for (i in 0 until size) {
            out.writeBoolean(value.value[i])
        }
    }

    override fun deserialize(input: DataInput2, available: Int): BooleanArrayValue {
        val vector = BooleanArray(size)
        for (i in 0 until size) {
            vector[i] = input.readBoolean()
        }
        return BooleanArrayValue(vector)
    }
}