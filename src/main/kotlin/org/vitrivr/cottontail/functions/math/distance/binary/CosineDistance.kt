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
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * A [VectorDistance] implementation to calculate the Cosine distance between a [query] and a series of [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class CosineDistance<T : VectorValue<*>>: VectorDistance<T> {
    /**
     * The [FunctionGenerator] for the [CosineDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        const val FUNCTION_NAME = "cosine"

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, arity = 1, Type.Double)

        override fun generateInternal(vararg arguments: Type<*>): Function.Dynamic<DoubleValue> = when (arguments[0]) {
            is Type.DoubleVector -> DoubleVector(arguments[0].logicalSize)
            is Type.FloatVector -> FloatVector(arguments[0].logicalSize)
            is Type.IntVector -> IntVector(arguments[0].logicalSize)
            is Type.LongVector -> LongVector(arguments[0].logicalSize)
            else -> throw FunctionNotSupportedException(this.signature)
        }
    }

    /** Name of this [CosineDistance]. */
    override val name: String = Generator.FUNCTION_NAME

    /** The cost of applying this [CosineDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = d * (6.0f * Cost.COST_FLOP + 4.0f * Cost.COST_MEMORY_ACCESS) + 4.0f * Cost.COST_FLOP + 3.0f * Cost.COST_MEMORY_ACCESS

    /**
     * [CosineDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : CosineDistance<DoubleVectorValue>() {
        override val type = Type.DoubleVector(size)
        override fun copy(d: Int) = DoubleVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as DoubleVectorValue
            val vector = arguments[1] as DoubleVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in query.data.indices) {
                dotp += (query.data[i] * vector.data[i])
                normq += query.data[i].pow(2)
                normv += vector.data[i].pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }

    /**
     * [CosineDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : CosineDistance<FloatVectorValue>() {
        override val type = Type.FloatVector(size)
        override fun copy(d: Int) = FloatVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as FloatVectorValue
            val vector = arguments[1] as FloatVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in query.data.indices) {
                dotp += (query.data[i] * vector.data[i])
                normq += query.data[i].pow(2)
                normv += vector.data[i].pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }

    /**
     * [CosineDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : CosineDistance<LongVectorValue>() {
        override val type = Type.LongVector(size)
        override fun copy(d: Int) = LongVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as LongVectorValue
            val vector = arguments[1] as LongVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in query.data.indices) {
                dotp += (query.data[i] * vector.data[i])
                normq += query.data[i].toDouble().pow(2)
                normv += vector.data[i].toDouble().pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }

    /**
     * [CosineDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : CosineDistance<IntVectorValue>() {
        override val type = Type.IntVector(size)
        override fun copy(d: Int) = IntVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as IntVectorValue
            val vector = arguments[1] as IntVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in query.data.indices) {
                dotp += (query.data[i] * vector.data[i])
                normq += query.data[i].toDouble().pow(2)
                normv += vector.data[i].toDouble().pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }
}