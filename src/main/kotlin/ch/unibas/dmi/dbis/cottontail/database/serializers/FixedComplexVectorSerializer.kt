package ch.unibas.dmi.dbis.cottontail.database.serializers

import ch.unibas.dmi.dbis.cottontail.model.values.ComplexVectorValue
import ch.unibas.dmi.dbis.cottontail.model.values.complex.ComplexArray
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
            out.writeComplex(value.value[i])
        }
    }

    override fun deserialize(input: DataInput2, available: Int): ComplexVectorValue {
        val vector = ComplexArray(size)
        for (i in 0 until size) {
            vector[i] = input.readComplex()
        }
        return ComplexVectorValue(vector)
    }
}