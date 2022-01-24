package org.vitrivr.cottontail.storage.serializers

import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.storage.serializers.mapdb.*

/**
 * A [ValueSerializerFactory] as used by Cottontail DB to create serializer implementations for its different storage engines.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object ValueSerializerFactory {
    /**
     * Returns the [MapDBSerializer] for the given [Types].
     *
     * @param type The [Types] to lookup a [MapDBSerializer] for.
     * @return [MapDBSerializer]
     */
    @Suppress("UNCHECKED_CAST")
    fun <T: Value> mapdb(type: Types<T>): MapDBSerializer<T> = when(type) {
        Types.Boolean -> BooleanValueMapDBSerializer
        Types.Date -> DateValueMapDBSerializer
        Types.Byte -> ByteValueMapDBSerializer
        Types.Complex32 -> Complex32ValueMapDBSerializer
        Types.Complex64 -> Complex64ValueMapDBSerializer
        Types.Double -> DoubleValueMapDBSerializer
        Types.Float -> FloatValueMapDBSerializer
        Types.Int -> IntValueMapDBSerializer
        Types.Long -> LongValueMapDBSerializer
        Types.Short -> ShortValueMapDBSerializer
        Types.String -> StringValueMapDBSerializer
        is Types.BooleanVector -> BooleanVectorValueMapDBSerializer(type.logicalSize)
        is Types.Complex32Vector -> Complex32VectorValueMapDBSerializer(type.logicalSize)
        is Types.Complex64Vector -> Complex64VectorValueMapDBSerializer(type.logicalSize)
        is Types.DoubleVector -> DoubleVectorMapDBSerializer(type.logicalSize)
        is Types.FloatVector -> FloatVectorMapDBValueSerializer(type.logicalSize)
        is Types.IntVector -> IntVectorValueMapDBSerializer(type.logicalSize)
        is Types.LongVector -> LongVectorValueMapDBSerializer(type.logicalSize)
    } as MapDBSerializer<T>
}