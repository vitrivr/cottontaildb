package org.vitrivr.cottontail.functions.math.distance.other

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.AbstractFunctionGenerator
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.basics.FunctionGenerator
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.basics.VectorDistance
import org.vitrivr.cottontail.functions.math.distance.binary.InnerProductDistance
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * A [VectorDistance] implementation to calculate the distance between a [VectorValue]s and
 * a hyperplane defined by w * x + b = 0, with w,x ∈ ℝ^n and b ∈ ℝ
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class HyperplaneDistance<T: VectorValue<*>>: VectorDistance<T> {

    /**
     * The [FunctionGenerator] for the [InnerProductDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        val FUNCTION_NAME = Name.FunctionName("hyperplane")

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, arity = 3, Type.Double)

        override fun generateInternal(vararg arguments: Type<*>): Function.Dynamic<DoubleValue> = when (arguments[0]) {
            is Type.DoubleVector -> DoubleVector(arguments[0].logicalSize)
            is Type.FloatVector -> FloatVector(arguments[0].logicalSize)
            is Type.LongVector -> LongVector(arguments[0].logicalSize)
            is Type.IntVector -> IntVector(arguments[0].logicalSize)
            else -> throw FunctionNotSupportedException(this.signature)
        }
    }

    /** Name of this [HyperplaneDistance]. */
    override val name: Name.FunctionName = Generator.FUNCTION_NAME

    /** The cost of applying this [InnerProductDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = this.d * (3.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS) + Cost.COST_FLOP

    /**
     * [HyperplaneDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : HyperplaneDistance<DoubleVectorValue>() {
        override val type = Type.DoubleVector(size)
        override fun copy(d: Int) = DoubleVector(d)

        /** The [Signature.Closed] of this [HyperplaneDistance] [Function]. */
        override val signature: Signature.Closed<out DoubleValue>
            get() = Signature.Closed(this.name, arrayOf(this.type, this.type, Type.Double), Type.Double)

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val q = arguments[0] as DoubleVectorValue
            val w = arguments[1] as DoubleVectorValue
            val b = arguments[2] as DoubleValue
            var dotp = 0.0
            var norm = 0.0
            for (i in 0 until q.logicalSize) {
                dotp += q.data[i] * w.data[i]
                norm += w.data[i].pow(2.0)
            }
            return DoubleValue(kotlin.math.abs(dotp + b.value) / sqrt(norm))
        }
    }

    /**
     * [HyperplaneDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : HyperplaneDistance<FloatVectorValue>() {
        override val type = Type.FloatVector(size)
        override fun copy(d: Int) = FloatVector(d)

        /** The [Signature.Closed] of this [HyperplaneDistance] [Function]. */
        override val signature: Signature.Closed<out DoubleValue>
            get() = Signature.Closed(this.name, arrayOf(this.type, this.type, Type.Float), Type.Double)

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val q = arguments[0] as FloatVectorValue
            val w = arguments[1] as FloatVectorValue
            val b = arguments[2] as FloatValue
            var dotp = 0.0
            var norm = 0.0
            for (i in 0 until q.logicalSize) {
                dotp += q.data[i] * w.data[i]
                norm += w.data[i].pow(2.0f)
            }
            return DoubleValue(kotlin.math.abs(dotp + b.value) / sqrt(norm))
        }
    }

    /**
     * [HyperplaneDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : HyperplaneDistance<LongVectorValue>() {
        override val type = Type.LongVector(size)
        override fun copy(d: Int) = LongVector(d)

        /** The [Signature.Closed] of this [HyperplaneDistance] [Function]. */
        override val signature: Signature.Closed<out DoubleValue>
            get() = Signature.Closed(this.name, arrayOf(this.type, this.type, Type.Float), Type.Double)

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val q = arguments[0] as LongVectorValue
            val w = arguments[1] as LongVectorValue
            val b = arguments[2] as LongValue
            var dotp = 0.0
            var norm = 0.0
            for (i in 0 until q.logicalSize) {
                dotp += q.data[i] * w.data[i]
                norm += w.data[i] * w.data[i]
            }
            return DoubleValue(kotlin.math.abs(dotp + b.value) / sqrt(norm))
        }
    }

    /**
     * [HyperplaneDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : HyperplaneDistance<IntVectorValue>() {
        override val type = Type.IntVector(size)
        override fun copy(d: Int) = IntVector(d)

        /** The [Signature.Closed] of this [HyperplaneDistance] [Function]. */
        override val signature: Signature.Closed<out DoubleValue>
            get() = Signature.Closed(this.name, arrayOf(this.type, this.type, Type.Int), Type.Double)

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val q = arguments[0] as IntVectorValue
            val w = arguments[1] as IntVectorValue
            val b = arguments[2] as IntValue
            var dotp = 0.0
            var norm = 0.0
            for (i in 0 until q.logicalSize) {
                dotp += q.data[i] * w.data[i]
                norm += w.data[i] * w.data[i]
            }
            return DoubleValue(kotlin.math.abs(dotp + b.value) / sqrt(norm))
        }
    }
}
