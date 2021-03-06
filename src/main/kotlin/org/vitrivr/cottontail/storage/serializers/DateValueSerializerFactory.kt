package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.model.values.DateValue
import org.vitrivr.cottontail.storage.serializers.mapdb.DateValueMapDBSerializer

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializers that can serialize and deserialize [DateValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DateValueSerializerFactory : ValueSerializerFactory<DateValue> {
    override fun mapdb(size: Int) = DateValueMapDBSerializer
}