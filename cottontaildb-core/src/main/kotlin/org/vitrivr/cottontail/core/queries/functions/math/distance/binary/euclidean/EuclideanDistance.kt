package org.vitrivr.cottontail.core.queries.functions.math.distance.binary.euclidean

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.*
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.MinkowskiDistance
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.*
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * A [EuclideanDistance] implementation to calculate the Euclidean or L2 distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class EuclideanDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): MinkowskiDistance<T>(type) {

    companion object: FunctionGenerator<DoubleValue> {
        val FUNCTION_NAME = Name.FunctionName.create("euclidean")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): Function<DoubleValue> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!"  }
            if (signature.arguments.any { it != signature.arguments[0] }) throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            return when(val type = signature.arguments[0].type) {
                is Types.Complex64Vector -> Complex64Vector(type)
                is Types.Complex32Vector -> Complex32Vector(type)
                is Types.DoubleVector -> DoubleVector(type)
                is Types.FloatVector -> FloatVector(type)
                is Types.HalfVector -> HalfVector(type)

                is Types.LongVector -> LongVector(type)
                is Types.IntVector -> IntVector(type)
                else -> throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            }
        }

        override fun resolve(signature: Signature.Open): List<Signature.Closed<*>> {
            if (Companion.signature != signature) throw FunctionNotSupportedException("Function generator ${Companion.signature} cannot generate function with signature $signature.")
            return listOf(
                Complex64Vector(Types.Complex64Vector(1)).signature,
                Complex32Vector(Types.Complex32Vector(1)).signature,
                DoubleVector(Types.DoubleVector(1)).signature,
                FloatVector(Types.FloatVector(1)).signature,
                LongVector(Types.LongVector(1)).signature,
                IntVector(Types.IntVector(1)).signature
            )
        }
    }

    /** The [Cost] of applying this [EuclideanDistance]. */
    override val cost: Cost by lazy {
        ((Cost.FLOP * 3.0f + Cost.MEMORY_ACCESS * 2.0f) * this.vectorSize) + Cost.OP_SQRT
    }

    /** The [EuclideanDistance] is a [MinkowskiDistance] with p = 2. */
    override val p: Int = 2

    /**
     * [EuclideanDistance] for a [Complex64VectorValue].
     */
    class Complex64Vector(type: Types.Vector<Complex64VectorValue,*>): EuclideanDistance<Complex64VectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = (arguments[0] as Complex64VectorValue).data
            val query = (arguments[1] as Complex64VectorValue).data
            var sum = 0.0
            for (i in 0 until 2 * this.vectorSize) {
                sum += (query[i] - probing[i]).pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = Complex64Vector(Types.Complex64Vector(d))
    }

    /**
     * [EuclideanDistance] for a [Complex32VectorValue].
     */
    class Complex32Vector(type: Types.Vector<Complex32VectorValue,*>): EuclideanDistance<Complex32VectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = (arguments[0] as Complex32VectorValue).data
            val query = (arguments[1] as Complex32VectorValue).data
            var sum = 0.0
            for (i in 0 until 2 * this.vectorSize) {
                sum += (query[i] - probing[i]).pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = Complex32Vector(Types.Complex32Vector(d))
    }

    /**
     * [EuclideanDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,*>): EuclideanDistance<DoubleVectorValue>(type), VectorisableFunction<DoubleValue>  {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = (arguments[0] as DoubleVectorValue).data
            val query = (arguments[1] as DoubleVectorValue).data
            var sum = 0.0
            for (i in 0 until this.vectorSize) {
                sum += (query[i] - probing[i]).pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))
        override fun vectorized() = EuclideanDistanceVectorised.DoubleVector(this.type)
    }

    /**
     * [EuclideanDistance] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,*>): EuclideanDistance<FloatVectorValue>(type), VectorisableFunction<DoubleValue> {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = (arguments[0] as FloatVectorValue).data
            val query = (arguments[1] as FloatVectorValue).data
            var sum = 0.0
            for (i in 0 until this.vectorSize) {
                sum += (query[i] - probing[i]).pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))
        override fun vectorized() = EuclideanDistanceVectorised.FloatVector(this.type)
    }

    /**
     * [EuclideanDistance] for a [FloatVectorValue].
     */
    class HalfVector(type: Types.Vector<HalfVectorValue,*>): EuclideanDistance<HalfVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = (arguments[0] as HalfVectorValue).data
            val query = (arguments[1] as HalfVectorValue).data
            var sum = 0.0
            for (i in 0 until this.vectorSize) {
                sum += (query[i] - probing[i]).pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = HalfVector(Types.HalfVector(d))
    }

    /**
     * [EuclideanDistance] for a [LongVectorValue].
     */
    class LongVector(type: Types.Vector<LongVectorValue,*>): EuclideanDistance<LongVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = (arguments[0] as LongVectorValue).data
            val query = (arguments[1] as LongVectorValue).data
            var sum = 0.0
            for (i in 0 until this.vectorSize) {
                sum += (query[i] - probing[i]).toDouble().pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = LongVector(Types.LongVector(d))
    }

    /**
     * [EuclideanDistance] for a [IntVectorValue].
     */
    class IntVector(type: Types.Vector<IntVectorValue,*>): EuclideanDistance<IntVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = (arguments[0] as IntVectorValue).data
            val query = (arguments[1] as IntVectorValue).data
            var sum = 0.0
            for (i in 0 until this.vectorSize) {
                sum += (query[i] - probing[i]).toDouble().pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = IntVector(Types.IntVector(d))
    }
}