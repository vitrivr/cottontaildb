package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

object DoubleValueSerializer : Serializer<DoubleValue> {
    override fun deserialize(input: DataInput2, available: Int): DoubleValue = DoubleValue(input.readDouble())
    override fun serialize(out: DataOutput2, value: DoubleValue) {
        out.writeDouble(value.value)
    }
}