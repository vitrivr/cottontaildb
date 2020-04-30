package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.values.BooleanVectorValue
import java.util.*


class FixedBooleanVectorSerializer(val size: Int) : Serializer<BooleanVectorValue> {
    override fun serialize(out: DataOutput2, value: BooleanVectorValue) {
        val words = value.value.toLongArray()
        for (i in 0 until words.size) {
            out.writeLong(words[i])
        }
    }

    override fun deserialize(input: DataInput2, available: Int): BooleanVectorValue {
        val words = LongArray((size + 63) / 64)
        for (i in 0 until words.size) {
            words[i] = input.readLong()
        }
        return BooleanVectorValue(BitSet.valueOf(words))
    }
}