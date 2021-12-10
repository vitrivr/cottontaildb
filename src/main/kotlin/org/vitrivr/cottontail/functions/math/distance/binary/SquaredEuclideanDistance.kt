package org.vitrivr.cottontail.functions.math.distance.binary

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.*
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.basics.MinkowskiDistance
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.pow

/**
 * A [SquaredEuclideanDistance] implementation to calculate the squared Euclidean or L2 distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed class SquaredEuclideanDistance<T : VectorValue<*>> : MinkowskiDistance<T> {

    /**
     * The [FunctionGenerator] for the [SquaredEuclideanDistance].
     */
    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        val FUNCTION_NAME = Name.FunctionName("squaredeuclidean")

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

    /** Name of this [SquaredEuclideanDistance]. */
    override val name: Name.FunctionName = Generator.FUNCTION_NAME

    /** The [p] value for an [SquaredEuclideanDistance] instance is always 2. */
    final override val p: Int = 2

    /** The cost of applying this [SquaredEuclideanDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = d * (3.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS) + Cost.COST_FLOP + Cost.COST_MEMORY_ACCESS

    /**
     * [SquaredEuclideanDistance] for a [Complex64VectorValue].
     */
    class Complex64Vector(size: Int) : SquaredEuclideanDistance<Complex64VectorValue>() {
        override val type = Type.Complex64Vector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = Complex64Vector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as Complex64VectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as Complex64VectorValue
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [Complex32VectorValue].
     */
    class Complex32Vector(size: Int) : SquaredEuclideanDistance<Complex32VectorValue>() {
        override val type = Type.Complex32Vector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = Complex32Vector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as Complex32VectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as Complex32VectorValue
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(size: Int) : SquaredEuclideanDistance<DoubleVectorValue>() {
        override val type = Type.DoubleVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = DoubleVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as DoubleVectorValue
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [FloatVectorValue].
     */
    class FloatVector(size: Int) : SquaredEuclideanDistance<FloatVectorValue>() {
        override val type = Type.FloatVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = FloatVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sum)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as FloatVectorValue
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [LongVectorValue].
     */
    class LongVector(size: Int) : SquaredEuclideanDistance<LongVectorValue>() {
        override val type = Type.LongVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = LongVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).toDouble().pow(2)
            }
            return DoubleValue(sum)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as LongVectorValue
        }
    }

    /**
     * [SquaredEuclideanDistance] for a [IntVectorValue].
     */
    class IntVector(size: Int) : SquaredEuclideanDistance<IntVectorValue>() {
        override val type = Type.IntVector(size)
        override var query = this.type.defaultValue()
        override fun copy(d: Int) = IntVector(d)
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            var sum = 0.0
            for (i in query.data.indices) {
                sum += (query.data[i] - probing.data[i]).toDouble().pow(2)
            }
            return DoubleValue(sum)
        }
        override fun prepare(vararg arguments: Value?) {
            require(arguments[0]?.type == this.type) { "Value of type ${arguments[0]?.type} cannot be applied as argument for ${this.signature}." }
            this.query = arguments[0] as IntVectorValue
        }
    }
}