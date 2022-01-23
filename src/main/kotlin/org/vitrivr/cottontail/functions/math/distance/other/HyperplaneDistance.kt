package org.vitrivr.cottontail.functions.math.distance.other

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.*
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.basics.VectorDistance
import org.vitrivr.cottontail.functions.math.distance.binary.ChisquaredDistance
import org.vitrivr.cottontail.functions.math.distance.binary.InnerProductDistance
import org.vitrivr.cottontail.functions.math.distance.other.HyperplaneDistance.Generator.FUNCTION_NAME
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.sqrt

/**
 * A [VectorDistance] implementation to calculate the distance between a [VectorValue]s and
 * a hyperplane defined by w * x + b = 0, with w,x ∈ ℝ^n and b ∈ ℝ
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class HyperplaneDistance<T: VectorValue<*>>(val type: Type<out T>)
    : AbstractFunction<DoubleValue>(Signature.Closed(FUNCTION_NAME, arrayOf(Argument.Typed(type), Argument.Typed(type), Argument.Typed(Type.Double)), Type.Double)) {

    /**
     * The [FunctionGenerator] for the [InnerProductDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        val FUNCTION_NAME = Name.FunctionName("hyperplane")

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(ChisquaredDistance.Generator.FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector, Argument.Numeric), Type.Double)

        override fun generateInternal(dst: Signature.Closed<*>): Function<DoubleValue> = when (val type = dst.arguments[0].type) {
            is Type.DoubleVector -> DoubleVector(type.logicalSize)
            is Type.FloatVector -> FloatVector(type.logicalSize)
            else -> throw FunctionNotSupportedException("Function generator signature ${this.signature} does not support destination signature (dst = $dst).")
        }
    }

    /** The dimensionality of this [VectorDistance]. */
    val d: Int
        get() = this.type.logicalSize

    /** The cost of applying this [InnerProductDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = this.d * (3.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS) + Cost.COST_FLOP

    /**
     * [HyperplaneDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : HyperplaneDistance<DoubleVectorValue>(Type.DoubleVector(size)) {
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as DoubleVectorValue
            val query = this.arguments[1] as DoubleVectorValue
            val bias = this.arguments[2] as DoubleValue
            var dotp = 0.0
            var norm = 0.0
            for (i in 0 until probing.logicalSize) {
                dotp += probing.data[i] * query.data[i]
                norm += query.data[i] * query.data[i]
            }
            return DoubleValue(dotp + bias.value / sqrt(norm))
        }
    }

    /**
     * [HyperplaneDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : HyperplaneDistance<FloatVectorValue>(Type.FloatVector(size)) {
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as DoubleVectorValue
            val query = this.arguments[1] as DoubleVectorValue
            val bias = this.arguments[2] as DoubleValue
            var dotp = 0.0
            var norm = 0.0
            for (i in 0 until probing.logicalSize) {
                dotp += probing.data[i] * query.data[i]
                norm += query.data[i] * query.data[i]
            }
            return DoubleValue(dotp + bias.value / norm)
        }
    }

    /**
     * [HyperplaneDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : HyperplaneDistance<LongVectorValue>(Type.LongVector(size)) {
        override fun invoke(): DoubleValue {
            val probing = this.arguments[0] as LongVectorValue
            val query = this.arguments[1] as LongVectorValue
            val bias = this.arguments[2] as DoubleValue
            var dotp = 0.0
            var norm = 0.0
            for (i in 0 until probing.logicalSize) {
                dotp += probing.data[i] * query.data[i]
                norm += query.data[i] * query.data[i]
            }
            return DoubleValue(dotp + bias.value / norm)
        }
    }

    /**
     * [HyperplaneDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : HyperplaneDistance<IntVectorValue>(Type.IntVector(size)) {
        override fun invoke(): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            val query = this.arguments[1] as IntVectorValue
            val bias = this.arguments[2] as DoubleValue
            var dotp = 0.0
            var norm = 0.0
            for (i in 0 until probing.logicalSize) {
                dotp += probing.data[i] * query.data[i]
                norm += query.data[i] * query.data[i]
            }
            return DoubleValue(dotp + bias.value / norm)
        }
    }
}
