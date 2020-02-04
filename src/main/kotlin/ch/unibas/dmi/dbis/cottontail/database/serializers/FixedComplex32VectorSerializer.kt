package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.Complex32VectorValue
import ch.unibas.dmi.dbis.cottontail.model.values.complex.Complex32
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * A [Serializer] for [Complex32VectorValue]s that a fixed in length.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class FixedComplex32VectorSerializer(val size: Int) : Serializer<Complex32VectorValue> {
    override fun serialize(out: DataOutput2, value: Complex32VectorValue) {
        for (i in 0 until size) {
            out.writeFloat(value.value[i * 2])
            out.writeFloat(value.value[i * 2 + 1])
        }
    }

    override fun deserialize(input: DataInput2, available: Int): Complex32VectorValue {
        val vector = FloatArray(size * 2)
        for (i in 0 until size) {
            vector[i * 2] = input.readFloat()
            vector[i * 2 + 1] = input.readFloat()
        }
        return Complex32VectorValue(vector)
    }
}