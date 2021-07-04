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
 * A [VectorDistance] implementation to calculate the inner product distance between [query] and a [VectorValue].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class InnerProductDistance<T : VectorValue<*>>: VectorDistance<T> {
    /**
     * The [FunctionGenerator] for the [InnerProductDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        const val FUNCTION_NAME = "innerproduct"

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, Type.Double, arity = 1)

        override fun generateInternal(vararg arguments: Type<*>): Function.Dynamic<DoubleValue> = when (arguments[0]) {
            is Type.Complex64Vector -> Complex64Vector(arguments[0].logicalSize)
            is Type.Complex32Vector -> Complex32Vector(arguments[0].logicalSize)
            is Type.DoubleVector -> DoubleVector(arguments[0].logicalSize)
            is Type.FloatVector -> FloatVector(arguments[0].logicalSize)
            is Type.LongVector -> LongVector(arguments[0].logicalSize)
            is Type.IntVector -> IntVector(arguments[0].logicalSize)
            else -> throw FunctionNotSupportedException(this.signature)
        }
    }


    /** Name of this [InnerProductDistance]. */
    override val name: String = Generator.FUNCTION_NAME

    /** The cost of applying this [InnerProductDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = this.d * (3.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS) + Cost.COST_FLOP

    /**
     * [EuclideanDistance] for a [Complex64VectorValue].
     */
    class Complex64Vector(size: Int) : EuclideanDistance<Complex64VectorValue>() {
        override val type = Type.Complex64Vector(size)
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as Complex64VectorValue
            var real = 0.0
            var imaginary = 0.0
            for (i in 0 until this.query.logicalSize) {
                val iprime = i shl 1
                real += this.query.data[iprime] * vector.data[iprime] + this.query.data[iprime + 1] * vector.data[iprime + 1]
                imaginary += this.query.data[iprime + 1] * vector.data[iprime] - this.query.data[iprime] * vector.data[iprime + 1]
            }
            return DoubleValue(1.0) - Complex64Value(real, imaginary).abs()
        }
    }

    /**
     * [EuclideanDistance] for a [Complex32VectorValue].
     */
    class Complex32Vector(size: Int) : EuclideanDistance<Complex32VectorValue>() {
        override val type = Type.Complex32Vector(size)
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as Complex32VectorValue
            var real = 0.0
            var imaginary = 0.0
            for (i in 0 until this.query.logicalSize) {
                val iprime = i shl 1
                real += this.query.data[iprime] * vector.data[iprime] + this.query.data[iprime + 1] * vector.data[iprime + 1]
                imaginary += this.query.data[iprime + 1] * vector.data[iprime] - this.query.data[iprime] * vector.data[iprime + 1]
            }
            return Complex64Value(real, imaginary).abs()
        }
    }

    /**
     * [EuclideanDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : EuclideanDistance<DoubleVectorValue>() {
        override val type = Type.DoubleVector(size)
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as DoubleVectorValue
            var dotp = 0.0
            for (i in this.query.data.indices) {
                dotp += this.query.data[i] * vector.data[i]
            }
            return DoubleValue(dotp)
        }
    }

    /**
     * [EuclideanDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : EuclideanDistance<FloatVectorValue>() {
        override val type = Type.FloatVector(size)
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as FloatVectorValue
            var dotp = 0.0
            for (i in this.query.data.indices) {
                dotp += this.query.data[i] * vector.data[i]
            }
            return DoubleValue(dotp)
        }
    }

    /**
     * [EuclideanDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : EuclideanDistance<LongVectorValue>() {
        override val type = Type.LongVector(size)
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as LongVectorValue
            var dotp = 0.0
            for (i in this.query.data.indices) {
                dotp += this.query.data[i] * vector.data[i]
            }
            return DoubleValue(dotp)
        }
    }

    /**
     * [EuclideanDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : EuclideanDistance<IntVectorValue>() {
        override val type = Type.IntVector(size)
        override var query = this.type.defaultValue()
        override fun invoke(vararg arguments: Value): DoubleValue {
            val vector = arguments[0] as IntVectorValue
            var dotp = 0.0
            for (i in this.query.data.indices) {
                dotp += this.query.data[i] * vector.data[i]
            }
            return DoubleValue(dotp)
        }
    }
}