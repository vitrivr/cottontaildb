package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.IntArrayValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * A [Serializer] for [IntArrayValue]s that are fixed in length.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FixedIntArraySerializer(val size: Int): Serializer<IntArrayValue> {
    override fun serialize(out: DataOutput2, value: IntArrayValue) {
        for (i in 0 until size) {
            out.writeInt(value.value[i])
        }
    }
    override fun deserialize(input: DataInput2, available: Int): IntArrayValue {
        val vector = IntArray(size)
        for (i in 0 until size) {
            vector[i] = input.readInt()
        }
        return IntArrayValue(vector)
    }
}