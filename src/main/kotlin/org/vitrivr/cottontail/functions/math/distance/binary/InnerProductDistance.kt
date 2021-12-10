package org.vitrivr.cottontail.functions.math.distance.binary

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.*
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.basics.VectorDistance
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A [VectorDistance.Binary] implementation to calculate the inner product distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class InnerProductDistance<T : VectorValue<*>>: VectorDistance.Binary<T> {
    /**
     * The [FunctionGenerator] for the [InnerProductDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        val FUNCTION_NAME = Name.FunctionName("innerproduct")

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector), Type.Double)

        override fun generateInternal(dst: Signature.Closed<*>): Function<DoubleValue> = when (val type = dst.arguments[0].type) {
            is Type.Complex64Vector -> Complex64Vector(type.logicalSize)
            is Type.Complex32Vector -> Complex32Vector(type.logicalSize)
            is Type.DoubleVector -> DoubleVector(type.logicalSize)
            is Type.FloatVector -> FloatVector(type.logicalSize)
            is Type.LongVector -> LongVector(type.logicalSize)
            is Type.IntVector -> IntVector(type.logicalSize)
            else -> throw FunctionNotSupportedException("Function generator signature ${this.signature} does not support destination signature (dst = $dst).")
        }
    }

    /** Name of this [InnerProductDistance]. */
    override val name: Name.FunctionName = Generator.FUNCTION_NAME

    /** The cost of applying this [InnerProductDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = this.d * (3.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS) + Cost.COST_FLOP

    /**
     * [InnerProductDistance] for a [Complex64VectorValue].
     */
    class Complex64Vector(size: Int) : InnerProductDistance<Complex64VectorValue>() {
        override val type = Type.Complex64Vector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = Complex64Vector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as Complex64VectorValue
            var real = 0.0
            var imaginary = 0.0
            for (i in 0 until query.logicalSize) {
                val iprime = i shl 1
                real += query.data[iprime] * probing.data[iprime] + query.data[iprime + 1] * probing.data[iprime + 1]
                imaginary += query.data[iprime + 1] * probing.data[iprime] - query.data[iprime] * probing.data[iprime + 1]
            }
            return DoubleValue(1.0) - Complex64Value(real, imaginary).abs()
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as Complex64VectorValue
        }
    }

    /**
     * [InnerProductDistance] for a [Complex32VectorValue].
     */
    class Complex32Vector(size: Int) : InnerProductDistance<Complex32VectorValue>() {
        override val type = Type.Complex32Vector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = Complex32Vector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as Complex32VectorValue
            var real = 0.0
            var imaginary = 0.0
            for (i in 0 until query.logicalSize) {
                val iprime = i shl 1
                real += query.data[iprime] * probing.data[iprime] + query.data[iprime + 1] * probing.data[iprime + 1]
                imaginary += query.data[iprime + 1] * probing.data[iprime] - query.data[iprime] * probing.data[iprime + 1]
            }
            return Complex64Value(real, imaginary).abs()
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as Complex32VectorValue
        }
    }

    /**
     * [InnerProductDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : InnerProductDistance<DoubleVectorValue>() {
        override val type = Type.DoubleVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = DoubleVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            var dotp = 0.0
            for (i in query.data.indices) {
                dotp += query.data[i] * probing.data[i]
            }
            return DoubleValue(dotp)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as DoubleVectorValue
        }
    }

    /**
     * [InnerProductDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : InnerProductDistance<FloatVectorValue>() {
        override val type = Type.FloatVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = FloatVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            var dotp = 0.0
            for (i in query.data.indices) {
                dotp += query.data[i] * probing.data[i]
            }
            return DoubleValue(dotp)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as FloatVectorValue
        }
    }

    /**
     * [InnerProductDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : InnerProductDistance<LongVectorValue>() {
        override val type = Type.LongVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = LongVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[1] as LongVectorValue
            var dotp = 0.0
            for (i in query.data.indices) {
                dotp += query.data[i] * probing.data[i]
            }
            return DoubleValue(dotp)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as LongVectorValue
        }
    }

    /**
     * [InnerProductDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : InnerProductDistance<IntVectorValue>() {
        override val type = Type.IntVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = IntVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            var dotp = 0.0
            for (i in query.data.indices) {
                dotp += query.data[i] * probing.data[i]
            }
            return DoubleValue(dotp)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as IntVectorValue
        }
    }
}