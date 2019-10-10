package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.ComplexValue
import ch.unibas.dmi.dbis.cottontail.model.values.complex.Complex
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

object ComplexValueSerializer : Serializer<ComplexValue> {
    override fun deserialize(input: DataInput2, available: Int): ComplexValue = ComplexValue(readComplex(input))
    override fun serialize(out: DataOutput2, value: ComplexValue) {
        out.writeFloat(value.value[0]) // real
        out.writeFloat(value.value[1]) // imaginary
    }

    private fun readComplex(input: DataInput2): Complex {
        val real: Float = input.readFloat()
        val imaginary: Float = input.readFloat()
        return Complex(floatArrayOf(real, imaginary))
    }
}