package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.FloatValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

object FloatValueSerializer : Serializer<FloatValue> {
    override fun deserialize(input: DataInput2, available: Int): FloatValue = FloatValue(input.readFloat())
    override fun serialize(out: DataOutput2, value: FloatValue) {
        out.writeFloat(value.value)
    }
}