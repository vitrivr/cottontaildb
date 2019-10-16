package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.ComplexValue
import ch.unibas.dmi.dbis.cottontail.model.values.complex.Complex
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

object ComplexValueSerializer : Serializer<ComplexValue> {
    override fun deserialize(input: DataInput2, available: Int): ComplexValue = ComplexValue(Complex(floatArrayOf(input.readFloat(), input.readFloat())))
    override fun serialize(out: DataOutput2, value: ComplexValue) {
        out.writeFloat(value.value[0])
        out.writeFloat(value.value[1])
    }
}