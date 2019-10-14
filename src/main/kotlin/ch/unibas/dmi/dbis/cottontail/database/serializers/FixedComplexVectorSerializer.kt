package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.ComplexVectorValue
import ch.unibas.dmi.dbis.cottontail.model.values.complex.Complex
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * A [Serializer] for [ComplexVectorValue]s that a fixed in length.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class FixedComplexVectorSerializer(val size: Int) : Serializer<ComplexVectorValue> {
    override fun serialize(out: DataOutput2, value: ComplexVectorValue) {
        for (i in 0 until size) {
            out.writeFloat(value.value[i][0]) // real
            out.writeFloat(value.value[i][1]) // imaginary
        }
    }

    override fun deserialize(input: DataInput2, available: Int): ComplexVectorValue {
        val vector = Array(size) { Complex(floatArrayOf(0.0f, 0.0f)) }
        for (i in 0 until size) {
            vector[i] = readComplex(input)
        }
        return ComplexVectorValue(vector)
    }

    private fun readComplex(input: DataInput2): Complex {
        // TODO
        val real: Float = input.readFloat()
        val imaginary: Float = input.readFloat()
        return Complex(floatArrayOf(real, imaginary))
    }
}