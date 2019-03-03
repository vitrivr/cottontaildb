package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.StringValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

object StringValueSerializer : Serializer<StringValue> {
    override fun deserialize(input: DataInput2, available: Int): StringValue = StringValue(input.readUTF())
    override fun serialize(out: DataOutput2, value: StringValue) {
        out.writeUTF(value.value)
    }
}