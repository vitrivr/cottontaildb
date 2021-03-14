package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.LongVectorValue
import org.vitrivr.cottontail.storage.serializers.mapdb.LongVectorValueMapDBSerializer
import org.vitrivr.cottontail.storage.serializers.mapdb.MapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [LongVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object LongVectorValueSerializerFactory : ValueSerializerFactory<LongVectorValue> {
    override fun mapdb(size: Int): MapDBSerializer<LongVectorValue> = LongVectorValueMapDBSerializer(size)
}