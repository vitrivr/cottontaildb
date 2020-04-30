package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.values.StringValue

object StringValueSerializer : Serializer<StringValue> {
    override fun deserialize(input: DataInput2, available: Int): StringValue = StringValue(input.readUTF())
    override fun serialize(out: DataOutput2, value: StringValue) {
        out.writeUTF(value.value)
    }
}