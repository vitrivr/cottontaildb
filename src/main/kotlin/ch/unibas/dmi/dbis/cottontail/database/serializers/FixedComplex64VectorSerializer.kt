package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.Complex64VectorValue
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * A [Serializer] for [Complex64VectorValue]s that a fixed in length.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class FixedComplex64VectorSerializer(val size: Int) : Serializer<Complex64VectorValue> {
    override fun serialize(out: DataOutput2, value: Complex64VectorValue) {
        for (i in 0 until size) {
            out.writeDouble(value.value[i * 2])
            out.writeDouble(value.value[i * 2 + 1])
        }
    }

    override fun deserialize(input: DataInput2, available: Int): Complex64VectorValue {
        val vector = DoubleArray(size * 2)
        for (i in 0 until size) {
            vector[i * 2] = input.readDouble()
            vector[i * 2 + 1] = input.readDouble()
        }
        return Complex64VectorValue(vector)
    }
}