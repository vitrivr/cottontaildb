package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.values.BooleanValue

object BooleanValueSerializer : Serializer<BooleanValue> {
    override fun deserialize(input: DataInput2, available: Int): BooleanValue = BooleanValue(input.readBoolean())
    override fun serialize(out: DataOutput2, value: BooleanValue) {
        out.writeBoolean(value.value)
    }
}