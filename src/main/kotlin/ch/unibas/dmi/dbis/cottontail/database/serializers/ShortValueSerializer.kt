package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.ShortValue

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

object ShortValueSerializer : Serializer<ShortValue> {
    override fun deserialize(input: DataInput2, available: Int): ShortValue = ShortValue(input.readShort())
    override fun serialize(out: DataOutput2, value: ShortValue) {
        out.writeShort(value.value.toInt())
    }
}