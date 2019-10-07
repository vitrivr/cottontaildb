package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.ComplexValue
import ch.unibas.dmi.dbis.cottontail.model.values.complex.Complex
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

object ComplexValueSerializer : Serializer<ComplexValue> {
    override fun deserialize(input: DataInput2, available: Int): ComplexValue = ComplexValue(input.readComplex())
    override fun serialize(out: DataOutput2, value: ComplexValue) {
        out.writeComplex(value.value)
    }
}

fun DataInput2.readComplex(): Complex {
    // TODO readComplex
    return Complex(0.0f, 0.0f)
}

fun DataOutput2.writeComplex(complex: Complex) {
    // TODO writeComplex
}