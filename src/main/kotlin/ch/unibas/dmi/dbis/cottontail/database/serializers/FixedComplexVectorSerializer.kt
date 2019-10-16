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
            out.writeFloat(value.value[i][0])
            out.writeFloat(value.value[i][1])
        }
    }

    override fun deserialize(input: DataInput2, available: Int): ComplexVectorValue {
        val vector = Array(size) { Complex(floatArrayOf(input.readFloat(), input.readFloat())) }
        return ComplexVectorValue(vector)
    }
}