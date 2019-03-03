package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.LongValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

object LongValueSerializer : Serializer<LongValue> {
    override fun deserialize(input: DataInput2, available: Int): LongValue = LongValue(input.readLong())
    override fun serialize(out: DataOutput2, value: LongValue) {
        out.writeLong(value.value)
    }
}