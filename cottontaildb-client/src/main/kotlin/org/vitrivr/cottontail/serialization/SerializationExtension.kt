package org.vitrivr.cottontail.serialization

import kotlinx.serialization.KSerializer
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.*

/**
 * Returns the [KSerializer] to encode/decode [PublicValue] of this [Types].
 *
 * @return [KSerializer]
 */
@Suppress("UNCHECKED_CAST")
fun Types<*>.valueSerializer(): KSerializer<PublicValue?> = when (this) {
    Types.Boolean -> BooleanValue.serializer()
    Types.ByteString -> ByteStringValue.serializer()
    Types.Date -> DateValue.serializer()
    Types.Byte -> ByteValue.serializer()
    Types.Complex32 -> Complex32Value.serializer()
    Types.Complex64 -> Complex64Value.serializer()
    Types.Double -> DoubleValue.serializer()
    Types.Float -> FloatValue.serializer()
    Types.Int -> IntValue.serializer()
    Types.Long -> LongValue.serializer()
    Types.Short -> ShortValue.serializer()
    Types.String -> StringValue.serializer()
    Types.Uuid -> UuidValue.serializer()
    is Types.BooleanVector -> BooleanVectorValue.serializer()
    is Types.Complex32Vector -> Complex32VectorValue.serializer()
    is Types.Complex64Vector -> Complex64VectorValue.serializer()
    is Types.DoubleVector -> DoubleVectorValue.serializer()
    is Types.FloatVector -> FloatVectorValue.serializer()
    is Types.IntVector -> IntVectorValue.serializer()
    is Types.LongVector -> LongVectorValue.serializer()
} as KSerializer<PublicValue?>


/**
 * Returns a [TupleSerializer] for a [List] of [ColumnDef].
 *
 * @return [TupleSerializer]
 */
fun Array<ColumnDef<*>>.valueSerializer() = TupleSerializer(this)

/**
 * Returns a [TupleSimpleSerializer] for a [List] of [ColumnDef].
 *
 * @return [TupleSimpleSerializer]
 */
fun Array<ColumnDef<*>>.descriptionSerializer() = TupleSimpleSerializer(this)

/**
 * Returns a [TupleSerializer] for a [Tuple].
 *
 * @return [KSerializer]
 */
fun Tuple.valueSerializer() = TupleSerializer(this.columns)

/**
 * Returns a [TupleSimpleSerializer] for a [Tuple].
 *
 * @return [TupleSimpleSerializer]
 */
fun Tuple.descriptionSerializer() = TupleSimpleSerializer(this.columns)


/**
 * Returns a [TupleSerializer] for a [TupleIterator].
 *
 * @return [TupleSimpleSerializer]
 */
fun TupleIterator.valueSerializer() = this.columns.valueSerializer()


/**
 * Returns a [TupleSimpleSerializer] for a [TupleIterator].
 *
 * @return [TupleSimpleSerializer]
 */
fun TupleIterator.descriptionSerializer() = this.columns.descriptionSerializer()