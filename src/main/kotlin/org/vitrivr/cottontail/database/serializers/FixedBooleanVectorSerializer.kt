package org.vitrivr.cottontail.database.serializers

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import org.vitrivr.cottontail.model.values.BooleanVectorValue


class FixedBooleanVectorSerializer(val size: Int): Serializer<BooleanVectorValue> {
    override fun serialize(out: DataOutput2, value: BooleanVectorValue) {
        TODO()
    }

    override fun deserialize(input: DataInput2, available: Int): BooleanVectorValue {
        TODO()
    }
}