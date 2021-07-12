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
import kotlin.math.absoluteValue
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * A [ManhattanDistance] implementation to calculate Manhattan or L1 distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class ManhattanDistance<T : VectorValue<*>>: VectorDistance.MinkowskiDistance<T> {

    /**
     * The [FunctionGenerator] for the [ManhattanDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        const val FUNCTION_NAME = "manhattan"

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, arity = 2, Type.Double)

        override fun generateInternal(vararg arguments: Type<*>): Function.Dynamic<DoubleValue> = when (arguments[0]) {
            is Type.Complex64Vector -> Complex64Vector(arguments[0].logicalSize)
            is Type.Complex32Vector -> Complex32Vector(arguments[0].logicalSize)
            is Type.DoubleVector -> DoubleVector(arguments[0].logicalSize)
            is Type.FloatVector -> FloatVector(arguments[0].logicalSize)
            is Type.IntVector -> IntVector(arguments[0].logicalSize)
            is Type.LongVector -> LongVector(arguments[0].logicalSize)
            else -> throw FunctionNotSupportedException(this.signature)
        }
    }

    /** The [p] value for an [ManhattanDistance] instance is always 2. */
    final override val p: Int = 1

    /** The cost of applying this [ManhattanDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = d * (2.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS)

    /** Name of this [ManhattanDistance]. */
    override val name: String = Generator.FUNCTION_NAME

    /**
     * [ManhattanDistance] for a [Complex64VectorValue].
     */
    class Complex64Vector(size: Int) : ManhattanDistance<Complex64VectorValue>() {
        override val type = Type.Complex64Vector(size)
        override fun copy(d: Int) = Complex64Vector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as Complex64VectorValue
            val vector = arguments[1] as Complex64VectorValue
            var sum = 0.0
            for (i in 0 until query.data.size / 2) {
                val diffReal = query.data[i shl 1] - vector.data[i shl 1]
                val diffImaginary = query.data[(i shl 1) + 1] - vector.data[(i shl 1) + 1]
                sum += sqrt(diffReal.pow(2) + diffImaginary.pow(2))
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ManhattanDistance] for a [Complex32VectorValue].
     */
    class Complex32Vector(size: Int) : ManhattanDistance<Complex32VectorValue>() {
        override val type = Type.Complex32Vector(size)
        override fun copy(d: Int) = Complex32Vector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as DoubleVectorValue
            val vector = arguments[1] as Complex32VectorValue
            var sum = 0.0
            for (i in 0 until query.data.size / 2) {
                val diffReal = query.data[i shl 1] - vector.data[i shl 1]
                val diffImaginary = query.data[(i shl 1) + 1] - vector.data[(i shl 1) + 1]
                sum += sqrt(diffReal.pow(2) + diffImaginary.pow(2))
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ManhattanDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : ManhattanDistance<DoubleVectorValue>() {
        override val type = Type.DoubleVector(size)
        override fun copy(d: Int) = DoubleVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as DoubleVectorValue
            val vector = arguments[1] as DoubleVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - vector.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ManhattanDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : ManhattanDistance<FloatVectorValue>() {
        override val type = Type.FloatVector(size)
        override fun copy(d: Int) = FloatVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as FloatVectorValue
            val vector = arguments[1] as FloatVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - vector.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ManhattanDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : ManhattanDistance<LongVectorValue>() {
        override val type = Type.LongVector(size)
        override fun copy(d: Int) = LongVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as LongVectorValue
            val vector = arguments[1] as LongVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - vector.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }

    /**
     * [ManhattanDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : ManhattanDistance<IntVectorValue>() {
        override val type = Type.IntVector(size)
        override fun copy(d: Int) = IntVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as IntVectorValue
            val vector = arguments[1] as IntVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - vector.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
    }
}