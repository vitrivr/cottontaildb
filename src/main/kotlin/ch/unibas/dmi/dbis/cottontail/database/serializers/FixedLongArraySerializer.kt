package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.LongArrayValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * A [Serializer] for [LongArrayValue]s that are fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedLongArraySerializer(val size: Int): Serializer<LongArrayValue> {
    override fun serialize(out: DataOutput2, value: LongArrayValue) {
        for (i in 0 until size) {
            out.writeLong(value.value[i])
        }
    }
    override fun deserialize(input: DataInput2, available: Int): LongArrayValue {
        val vector = LongArray(size)
        for (i in 0 until size) {
            vector[i] = input.readLong()
        }
        return LongArrayValue(vector)
    }
}