package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.BooleanValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

object BooleanValueSerializer : Serializer<BooleanValue> {
    override fun deserialize(input: DataInput2, available: Int): BooleanValue = BooleanValue(input.readBoolean())
    override fun serialize(out: DataOutput2, value: BooleanValue) {
        out.writeBoolean(value.value)
    }
}