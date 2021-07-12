package org.vitrivr.cottontail.functions.math.distance.binary

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.AbstractFunctionGenerator
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.basics.FunctionGenerator
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.VectorDistance
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A [VectorDistance] implementation to calculate the Cosine distance between a [query] and a series of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class HammingDistance<T : VectorValue<*>>: VectorDistance<T> {
    /**
     * The [FunctionGenerator] for the [HammingDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        const val FUNCTION_NAME = "hamming"

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, arity = 2, Type.Double)

        override fun generateInternal(vararg arguments: Type<*>): Function.Dynamic<DoubleValue> = when (arguments[0]) {
            is Type.DoubleVector -> DoubleVector(arguments[0].logicalSize)
            is Type.FloatVector -> FloatVector(arguments[0].logicalSize)
            is Type.IntVector -> IntVector(arguments[0].logicalSize)
            is Type.LongVector -> LongVector(arguments[0].logicalSize)
            is Type.BooleanVector -> BooleanVector(arguments[0].logicalSize)
            else -> throw FunctionNotSupportedException(this.signature)
        }
    }

    /** The cost of applying this [HammingDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = d * (Cost.COST_FLOP + Cost.COST_MEMORY_ACCESS)

    /** Name of this [HammingDistance]. */
    override val name: String = Generator.FUNCTION_NAME

    /**
     * [HammingDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : HammingDistance<DoubleVectorValue>() {
        override val type = Type.DoubleVector(size)
        override fun copy(d: Int) = DoubleVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as DoubleVectorValue
            val vector = arguments[1] as DoubleVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != vector.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [HammingDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : HammingDistance<FloatVectorValue>() {
        override val type = Type.FloatVector(size)
        override fun copy(d: Int): VectorDistance<FloatVectorValue> = FloatVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as FloatVectorValue
            val vector = arguments[1] as FloatVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != vector.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [HammingDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : HammingDistance<LongVectorValue>() {
        override val type = Type.LongVector(size)
        override fun copy(d: Int): VectorDistance<LongVectorValue> = LongVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as LongVectorValue
            val vector = arguments[1] as LongVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != vector.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [HammingDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : HammingDistance<IntVectorValue>() {
        override val type = Type.IntVector(size)
        override fun copy(d: Int) = IntVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as IntVectorValue
            val vector = arguments[1] as IntVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != vector.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [HammingDistance] for a [IntVectorValue].
     */
    class BooleanVector(size: Int) : HammingDistance<BooleanVectorValue>() {
        override val type = Type.BooleanVector(size)
        override fun copy(d: Int) = BooleanVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as BooleanVectorValue
            val vector = arguments[1] as BooleanVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                if (query.data[i] != vector.data[i]) {
                    sum += 1.0
                }
            }
            return DoubleValue(sum)
        }
    }
}