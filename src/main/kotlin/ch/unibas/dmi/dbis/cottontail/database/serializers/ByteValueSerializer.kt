package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.ByteValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

object ByteValueSerializer : Serializer<ByteValue> {
    override fun deserialize(input: DataInput2, available: Int): ByteValue = ByteValue(input.readByte())
    override fun serialize(out: DataOutput2, value: ByteValue) {
        out.writeByte(value.value.toInt())
    }
}