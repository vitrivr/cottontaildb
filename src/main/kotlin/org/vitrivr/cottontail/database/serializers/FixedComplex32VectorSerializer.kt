package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.values.Complex32Value
import org.vitrivr.cottontail.model.values.Complex32VectorValue
import org.vitrivr.cottontail.model.values.FloatValue

/**
 * A [Serializer] for [Complex32VectorValue]s that a fixed in length.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class FixedComplex32VectorSerializer(val size: Int) : Serializer<Complex32VectorValue> {
    override fun serialize(out: DataOutput2, value: Complex32VectorValue) {
        for (i in 0 until size) {
            out.writeFloat(value.real(i).value)
            out.writeFloat(value.imaginary(i).value)
        }
    }

    override fun deserialize(input: DataInput2, available: Int): Complex32VectorValue {
        val vector = Array(this.size) {
            Complex32Value(FloatValue(input.readFloat()), FloatValue(input.readFloat()))
        }
        return Complex32VectorValue(vector)
    }
}