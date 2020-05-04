package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.values.Complex64Value

object Complex64ValueSerializer : Serializer<Complex64Value> {
    override fun deserialize(input: DataInput2, available: Int): Complex64Value = Complex64Value(doubleArrayOf(input.readDouble(), input.readDouble()))
    override fun serialize(out: DataOutput2, value: Complex64Value) {
        out.writeDouble(value.real.value)
        out.writeDouble(value.imaginary.value)
    }
}