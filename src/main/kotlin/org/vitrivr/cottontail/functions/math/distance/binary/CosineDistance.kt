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
            get() = Signature.Open(FUNCTION_NAME, Type.Double, arity = 1)

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
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as DoubleVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in this.query.data.indices) {
                dotp += (this.query.data[i] * vector.data[i])
                normq += this.query.data[i].pow(2)
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
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as FloatVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in this.query.data.indices) {
                dotp += (this.query.data[i] * vector.data[i])
                normq += this.query.data[i].pow(2)
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
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as LongVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in this.query.data.indices) {
                dotp += (this.query.data[i] * vector.data[i])
                normq += this.query.data[i].toDouble().pow(2)
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
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as IntVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in this.query.data.indices) {
                dotp += (this.query.data[i] * vector.data[i])
                normq += this.query.data[i].toDouble().pow(2)
                normv += vector.data[i].toDouble().pow(2)
            }
            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
    }
}