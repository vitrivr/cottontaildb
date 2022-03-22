package org.vitrivr.cottontail.dbms.functions.math.distance.binary

import jdk.incubator.vector.VectorOperators
import jdk.incubator.vector.VectorSpecies
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.functions.math.MinkowskiDistance
import org.vitrivr.cottontail.core.queries.functions.math.VectorDistance
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue
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
        val FUNCTION_NAME = Name.FunctionName("euclidean")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): Function<DoubleValue> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!"  }
            if (signature.arguments.any { it != signature.arguments[0] }) throw FunctionNotSupportedException("Function generator ${HaversineDistance.signature} cannot generate function with signature $signature.")
            return when(val type = signature.arguments[0].type) {
                is Types.Complex64Vector -> Complex64Vector(type).vectorized()
                is Types.Complex32Vector -> Complex32Vector(type).vectorized()
                is Types.DoubleVector -> DoubleVector(type).vectorized()
                is Types.FloatVector -> FloatVector(type).vectorized()
                is Types.LongVector -> LongVector(type).vectorized()
                is Types.IntVector -> IntVector(type).vectorized()
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
    override val cost: Cost
        get() = ((Cost.FLOP * 3.0f + Cost.MEMORY_ACCESS * 2.0f) * this.d) + Cost.OP_SQRT

    /** The [EuclideanDistance] is a [MinkowskiDistance] with p = 2. */
    override val p: Int
        get() = 2

    /**
     * [EuclideanDistance] for a [Complex64VectorValue].
     */
    class Complex64Vector(type: Types.Vector<Complex64VectorValue,*>): EuclideanDistance<Complex64VectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as Complex64VectorValue
            val query = arguments[1] as Complex64VectorValue
            var sum = 0.0
            for (i in 0 until 2 * this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = Complex64Vector(Types.Complex64Vector(d))

        override fun vectorized(): VectorDistance<Complex64VectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }

    /**
     * [EuclideanDistance] for a [Complex32VectorValue].
     */
    class Complex32Vector(type: Types.Vector<Complex32VectorValue,*>): EuclideanDistance<Complex32VectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as Complex32VectorValue
            val query = arguments[1] as Complex32VectorValue
            var sum = 0.0
            for (i in 0 until 2 * this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = Complex32Vector(Types.Complex32Vector(d))

        override fun vectorized(): VectorDistance<Complex32VectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }

    /**
     * [EuclideanDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,*>): EuclideanDistance<DoubleVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            var sum = 0.0
            for (i in 0 until this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))

        override fun vectorized(): VectorDistance<DoubleVectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }

    /**
     * [EuclideanDistance] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,*>): EuclideanDistance<FloatVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var sum = 0.0
            for (i in 0 until this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))

        override fun vectorized(): VectorDistance<FloatVectorValue> {
            return FloatVectorVectorized(type)
        }
    }

    /**
     * SIMD implementation: [EuclideanDistance] for a [FloatVectorValue]
     */
    class FloatVectorVectorized(type: Types.Vector<FloatVectorValue,*>): EuclideanDistance<FloatVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            // Changing SPECIES to SPECIES.PREFERRED results in a HUGE performance decrease
            val species: VectorSpecies<Float> = jdk.incubator.vector.FloatVector.SPECIES_256
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var vectorSum = jdk.incubator.vector.FloatVector.zero(species)

            //Vectorized calculation
            for (i in 0 until species.loopBound(this.d) step species.length()) {
                val vp = jdk.incubator.vector.FloatVector.fromArray(species, probing.data, i)
                val vq = jdk.incubator.vector.FloatVector.fromArray(species, query.data, i)
                vectorSum = vectorSum.lanewise(VectorOperators.ADD, vp.lanewise(VectorOperators.SUB, vq).pow(2f))
            }

            var sum = vectorSum.reduceLanes(VectorOperators.ADD)

            // Scalar calculation for the remaining lanes, since SPECIES.loopBound(this.d) <= this.d
            for (i in species.loopBound(this.d) until this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }

            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))

        override fun vectorized(): VectorDistance<FloatVectorValue> {
            return this
        }
    }

    /**
     * [EuclideanDistance] for a [LongVectorValue].
     */
    class LongVector(type: Types.Vector<LongVectorValue,*>): EuclideanDistance<LongVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            val query = arguments[1] as LongVectorValue
            var sum = 0.0
            for (i in 0 until this.d) {
                sum += (query.data[i] - probing.data[i]).toDouble().pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = LongVector(Types.LongVector(d))

        override fun vectorized(): VectorDistance<LongVectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }

    /**
     * [EuclideanDistance] for a [IntVectorValue].
     */
    class IntVector(type: Types.Vector<IntVectorValue,*>): EuclideanDistance<IntVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            val query = arguments[1] as IntVectorValue
            var sum = 0.0
            for (i in 0 until this.d) {
                sum += (query.data[i] - probing.data[i]).toDouble().pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = IntVector(Types.IntVector(d))

        override fun vectorized(): VectorDistance<IntVectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }
}