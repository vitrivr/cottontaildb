package org.vitrivr.cottontail.core.queries.functions.math.distance.binary

import jdk.incubator.vector.FloatVector.SPECIES_PREFERRED
import jdk.incubator.vector.VectorOperators
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.*
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.*
import kotlin.math.absoluteValue
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * A [ManhattanDistance] implementation to calculate Manhattan or L1 distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class ManhattanDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): MinkowskiDistance<T>(type) {

    companion object: FunctionGenerator<DoubleValue> {
        val FUNCTION_NAME = Name.FunctionName("manhattan")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): Function<DoubleValue> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!"  }
            if (signature.arguments.any { it != signature.arguments[0] }) throw FunctionNotSupportedException("Function generator ${HaversineDistance.signature} cannot generate function with signature $signature.")
            return when(val type = signature.arguments[0].type) {
                is Types.Complex64Vector -> Complex64Vector(type)
                is Types.Complex32Vector -> Complex32Vector(type)
                is Types.DoubleVector -> DoubleVector(type)
                is Types.FloatVector -> FloatVector(type)
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

    /** The [Cost] of applying this [ManhattanDistance]. */
    override val cost: Cost
        get() = ((Cost.FLOP * 2.0f + Cost.MEMORY_ACCESS * 2.0f) * this.vectorSize) + Cost.MEMORY_ACCESS

    /** The [ManhattanDistance] is a [MinkowskiDistance] with p = 1. */
    override val p: Int
        get() = 2

    /**
     * [ManhattanDistance] for a [Complex64VectorValue].
     */
    class Complex64Vector(type: Types.Vector<Complex64VectorValue,*>): ManhattanDistance<Complex64VectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as Complex64VectorValue
            val query = arguments[1] as Complex64VectorValue
            var sum = 0.0
            for (i in 0 until this.vectorSize) {
                val diffReal = query.data[i shl 1] - probing.data[i shl 1]
                val diffImaginary = query.data[(i shl 1) + 1] - probing.data[(i shl 1) + 1]
                sum += sqrt(diffReal.pow(2) + diffImaginary.pow(2))
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = Complex64Vector(Types.Complex64Vector(d))
    }

    /**
     * [ManhattanDistance] for a [Complex32VectorValue].
     */
    class Complex32Vector(type: Types.Vector<Complex32VectorValue,*>): ManhattanDistance<Complex32VectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as Complex32VectorValue
            val query = arguments[1] as Complex32VectorValue
            var sum = 0.0
            for (i in 0 until this.vectorSize) {
                val diffReal = query.data[i shl 1] - probing.data[i shl 1]
                val diffImaginary = query.data[(i shl 1) + 1] - probing.data[(i shl 1) + 1]
                sum += sqrt(diffReal.pow(2) + diffImaginary.pow(2))
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = Complex32Vector(Types.Complex32Vector(d))
    }

    /**
     * [ManhattanDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,*>): ManhattanDistance<DoubleVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            var sum = 0.0
            for (i in 0 until this.vectorSize) {
                sum += (query.data[i] - probing.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))
    }

    /**
     * [ManhattanDistance] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,*>): ManhattanDistance<FloatVectorValue>(type), VectorisableFunction<DoubleValue> {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var sum = 0.0
            for (i in 0 until this.vectorSize) {
                sum += (query.data[i] - probing.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))
        override fun vectorized() = FloatVectorVectorized(this.type)
    }

    /**
     * SIMD implementation: [ManhattanDistance] for a [FloatVectorValue].
     */
    class FloatVectorVectorized(type: Types.Vector<FloatVectorValue,*>): ManhattanDistance<FloatVectorValue>(type), VectorisedFunction<DoubleValue> {
        override val name: Name.FunctionName = FUNCTION_NAME

        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = (arguments[0] as FloatVectorValue).data
            val query = (arguments[1] as FloatVectorValue).data
            var vectorSum = jdk.incubator.vector.FloatVector.zero(SPECIES_PREFERRED)

            /* Vectorised distance calculation. */
            val bound = SPECIES_PREFERRED.loopBound(this.vectorSize)
            for (i in 0 until bound step SPECIES_PREFERRED.length()) {
                val vp = jdk.incubator.vector.FloatVector.fromArray(SPECIES_PREFERRED, probing, i)
                val vq = jdk.incubator.vector.FloatVector.fromArray(SPECIES_PREFERRED, query, i)
                vectorSum = vectorSum.add(vp.sub(vq).abs())
            }

            /* Scalar version for remainder. */
            var sum = vectorSum.reduceLanes(VectorOperators.ADD)
            for (i in bound until this.vectorSize) {
                sum += (query[i] - probing[i]).absoluteValue
            }

            return DoubleValue(sum)
        }
        override fun copy(d: Int) = FloatVectorVectorized(Types.FloatVector(d))
    }

    /**
     * [ManhattanDistance] for a [LongVectorValue].
     */
    class LongVector(type: Types.Vector<LongVectorValue,*>): ManhattanDistance<LongVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            val query = arguments[1] as LongVectorValue
            var sum = 0.0
            for (i  in 0 until this.vectorSize) {
                sum += (query.data[i] - probing.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = LongVector(Types.LongVector(d))
    }

    /**
     * [ManhattanDistance] for a [IntVectorValue].
     */
    class IntVector(type: Types.Vector<IntVectorValue,*>): ManhattanDistance<IntVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            val query = arguments[1] as IntVectorValue
            var sum = 0.0
            for (i in 0 until this.vectorSize) {
                sum += (query.data[i] - probing.data[i]).absoluteValue
            }
            return DoubleValue(sum)
        }
        override fun copy(d: Int) = IntVector(Types.IntVector(d))
    }
}