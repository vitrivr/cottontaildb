package org.vitrivr.cottontail.functions.math.distance.other

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.*
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.basics.VectorDistance
import org.vitrivr.cottontail.functions.math.distance.binary.ChisquaredDistance
import org.vitrivr.cottontail.functions.math.distance.binary.InnerProductDistance
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
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
            get() = Signature.Open(ChisquaredDistance.Generator.FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector, Argument.Numeric), Type.Double)

        override fun generateInternal(dst: Signature.Closed<*>): Function<DoubleValue> = when (val type = dst.arguments[0].type) {
            is Type.DoubleVector -> DoubleVector(type.logicalSize)
            is Type.FloatVector -> FloatVector(type.logicalSize)
            is Type.LongVector -> LongVector(type.logicalSize)
            is Type.IntVector -> IntVector(type.logicalSize)
            else -> throw FunctionNotSupportedException("Function generator signature ${this.signature} does not support destination signature (dst = $dst).")
        }
    }



    /** The query [VectorValue] for this hyperplane distance. */
    abstract var query: T

    /** Name of this [HyperplaneDistance]. */

    override val name: Name.FunctionName = Generator.FUNCTION_NAME

    /** By convention, the argument at position 1 (query argument) and position 2 (support argument / bias) is stateful for [HyperplaneDistance]. */
    override val statefulArguments: IntArray
        get() = intArrayOf(1, 2)

    /** The cost of applying this [InnerProductDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = this.d * (3.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS) + Cost.COST_FLOP

    /**
     * [HyperplaneDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : HyperplaneDistance<DoubleVectorValue>() {
        override val type = Type.DoubleVector(size)
        override var query = this.type.defaultValue()
        var bias = DoubleValue.ZERO
            private set
        private var cachedNormSqrt = 0.0

        override fun copy(d: Int) = DoubleVector(d)

        /** The [Signature.Closed] of this [HyperplaneDistance] [Function]. */
        override val signature: Signature.Closed<out DoubleValue>
            get() = Signature.Closed(this.name, arrayOf(Argument.Typed(this.type), Argument.Typed(this.type), Argument.Typed(Type.Double)), Type.Double)

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            var dotp = 0.0
            for (i in 0 until probing.logicalSize) {
                dotp += probing.data[i] * query.data[i]
            }
            return DoubleValue(dotp + bias.value / sqrt(cachedNormSqrt))
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            require(arguments[1]?.type == Type.Double) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as DoubleVectorValue
            this.bias = arguments[1] as DoubleValue
            var norm = 0.0
            for (i in 0 until this.query.logicalSize) {
                norm += query.data[i] * query.data[i]
            }
            this.cachedNormSqrt = sqrt(norm)
        }
    }

    /**
     * [HyperplaneDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : HyperplaneDistance<FloatVectorValue>() {
        override val type = Type.FloatVector(size)
        override var query = this.type.defaultValue()
        var bias = FloatValue.ZERO
            private set
        private var cachedNormSqrt = 0.0f

        override fun copy(d: Int) = FloatVector(d)

        /** The [Signature.Closed] of this [HyperplaneDistance] [Function]. */
        override val signature: Signature.Closed<out DoubleValue>
            get() = Signature.Closed(this.name, arrayOf(Argument.Typed(this.type), Argument.Typed(this.type), Argument.Typed(Type.Float)), Type.Double)

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            var dotp = 0.0
            for (i in 0 until probing.logicalSize) {
                dotp += probing.data[i] * query.data[i]
            }
            return DoubleValue(dotp + bias.value / cachedNormSqrt)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            require(arguments[1]?.type == Type.Double) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as FloatVectorValue
            this.bias = arguments[1] as FloatValue
            var norm = 0.0f
            for (i in 0 until this.query.logicalSize) {
                norm += query.data[i] * query.data[i]
            }
            this.cachedNormSqrt = sqrt(norm)
        }
    }

    /**
     * [HyperplaneDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : HyperplaneDistance<LongVectorValue>() {
        override val type = Type.LongVector(size)
        override var query = this.type.defaultValue()
        var bias = LongValue.ZERO
            private set
        private var cachedNormSqrt = 0L
        override fun copy(d: Int) = LongVector(d)

        /** The [Signature.Closed] of this [HyperplaneDistance] [Function]. */
        override val signature: Signature.Closed<out DoubleValue>
            get() = Signature.Closed(this.name, arrayOf(Argument.Typed(this.type), Argument.Typed(this.type), Argument.Typed(Type.Long)), Type.Double)

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            var dotp = 0.0
            for (i in 0 until probing.logicalSize) {
                dotp += probing.data[i] * query.data[i]
            }
            return DoubleValue(dotp + bias.value / cachedNormSqrt)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            require(arguments[1]?.type == Type.Double) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as LongVectorValue
            this.bias = arguments[1] as LongValue
            var norm = 0.0
            for (i in 0 until this.query.logicalSize) {
                norm += query.data[i] * query.data[i]
            }
            this.cachedNormSqrt = sqrt(norm).toLong()
        }
    }

    /**
     * [HyperplaneDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : HyperplaneDistance<IntVectorValue>() {
        override val type = Type.IntVector(size)
        override var query = this.type.defaultValue()
        var bias = IntValue.ZERO
            private set
        private var cachedNormSqrt = 0
        override fun copy(d: Int) = IntVector(d)

        /** The [Signature.Closed] of this [HyperplaneDistance] [Function]. */
        override val signature: Signature.Closed<out DoubleValue>
            get() = Signature.Closed(this.name, arrayOf(Argument.Typed(this.type), Argument.Typed(this.type), Argument.Typed(Type.Int)), Type.Double)

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            var dotp = 0.0
            for (i in 0 until probing.logicalSize) {
                dotp += probing.data[i] * query.data[i]
            }
            return DoubleValue(dotp + bias.value / cachedNormSqrt)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            require(arguments[1]?.type == Type.Double) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as IntVectorValue
            this.bias = arguments[1] as IntValue
            var norm = 0.0f
            for (i in 0 until this.query.logicalSize) {
                norm += query.data[i] * query.data[i]
            }
            this.cachedNormSqrt = sqrt(norm).toInt()
        }
    }
}
