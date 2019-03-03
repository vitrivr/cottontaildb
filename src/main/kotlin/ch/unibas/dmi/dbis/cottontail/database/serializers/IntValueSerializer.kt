package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.IntValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

object IntValueSerializer : Serializer<IntValue> {
    override fun deserialize(input: DataInput2, available: Int): IntValue = IntValue(input.readInt())
    override fun serialize(out: DataOutput2, value: IntValue) {
        out.writeInt(value.value)
    }
}