package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.values.Complex64VectorValue

/**
 * A [Serializer] for [Complex64VectorValue]s that a fixed in length.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class FixedComplex64VectorSerializer(val size: Int) : Serializer<Complex64VectorValue> {
    override fun serialize(out: DataOutput2, value: Complex64VectorValue) {
        for (i in 0 until size) {
            out.writeDouble(value.real(i).value)
            out.writeDouble(value.imaginary(i).value)
        }
    }

    override fun deserialize(input: DataInput2, available: Int): Complex64VectorValue {
        val vector = DoubleArray(2 * this.size) { input.readDouble() }
        return Complex64VectorValue(vector)
    }
}