package org.vitrivr.cottontail.test

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.generators.*
import java.util.random.RandomGenerator


/**
 * Generates a random [Tuple] for the schema provided by the [ColumnDef]  array.
 *
 * @param tupleId The [TupleId] of the [Tuple] to generate.
 * @param generator The [RandomGenerator] instance to use.
 *
 * @return [Tuple]
 */
fun Array<ColumnDef<*>>.randomTuple(tupleId: TupleId, generator: RandomGenerator): Tuple {
    val values: Array<Value?> = this.map {
        /* Handle nullable columns. */

        if (it.nullable && generator.nextBoolean()) {
            return@map null
        }

        /* Handle auto-increment columns. */
        if (it.autoIncrement) {
            return@map when(it.type) {
                Types.Int -> IntValue(tupleId.toInt())
                Types.Long -> LongValue(tupleId)
                else -> throw IllegalStateException("Only INT and LONG are supported for auto-increment columns.")
            }
        }

        /* Handle all the other columns. **/
        when (val type = it.type) {
            Types.Boolean -> BooleanValueGenerator.random(generator)
            Types.ByteString -> TODO()
            Types.Date -> DateValueGenerator.random(generator)
            Types.Byte -> ByteValueGenerator.random(generator)
            Types.Complex32 -> Complex32ValueGenerator.random(generator)
            Types.Complex64 -> Complex64ValueGenerator.random(generator)
            Types.Double -> DoubleValueGenerator.random(generator)
            Types.Float -> FloatValueGenerator.random(generator)
            Types.Int -> IntValueGenerator.random(generator)
            Types.Long -> LongValueGenerator.random(generator)
            Types.Short -> ShortValueGenerator.random(generator)
            Types.String -> StringValueGenerator.random(generator.nextInt(1, 15), generator)
            is Types.BooleanVector -> BooleanVectorValueGenerator.random(it.type.logicalSize, generator)
            is Types.Complex32Vector -> Complex32VectorValueGenerator.random(it.type.logicalSize, generator)
            is Types.Complex64Vector -> Complex64VectorValueGenerator.random(it.type.logicalSize, generator)
            is Types.DoubleVector -> DoubleVectorValueGenerator.random(it.type.logicalSize, generator)
            is Types.FloatVector -> FloatVectorValueGenerator.random(it.type.logicalSize, generator)
            is Types.IntVector -> IntVectorValueGenerator.random(it.type.logicalSize, generator)
            is Types.LongVector -> LongVectorValueGenerator.random(it.type.logicalSize, generator)
            else -> throw IllegalArgumentException("Cannot generate random value for type $type.")
        }
    }.toTypedArray()
    return StandaloneTuple(tupleId, this, values)
}