package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.PublicValue

/**
 * Generates and returns the default [PublicValue] for the given [Types].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
fun Types<*>.defaultValue(): PublicValue = when(this) {
    Types.Boolean -> BooleanValueGenerator.ofFalse()
    Types.ByteString -> ByteStringValueGenerator.empty()
    Types.Date -> DateValueGenerator.now()
    Types.Byte -> ByteValueGenerator.zero()
    Types.Complex32 -> Complex32ValueGenerator.zero()
    Types.Complex64 -> Complex64ValueGenerator.zero()
    Types.Double -> DoubleValueGenerator.zero()
    Types.Float -> FloatValueGenerator.zero()
    Types.Int -> IntValueGenerator.zero()
    Types.Long -> LongValueGenerator.zero()
    Types.Short -> ShortValueGenerator.zero()
    Types.String -> StringValueGenerator.empty()
    Types.Uuid -> UuidValueGenerator.NIL
    is Types.BooleanVector -> BooleanVectorValueGenerator.zero(this.logicalSize)
    is Types.Complex32Vector -> Complex32VectorValueGenerator.zero(this.logicalSize)
    is Types.Complex64Vector -> Complex64VectorValueGenerator.zero(this.logicalSize)
    is Types.DoubleVector -> DoubleVectorValueGenerator.zero(this.logicalSize)
    is Types.FloatVector -> FloatVectorValueGenerator.zero(this.logicalSize)
    is Types.IntVector -> IntVectorValueGenerator.zero(this.logicalSize)
    is Types.LongVector -> LongVectorValueGenerator.zero(this.logicalSize)
}