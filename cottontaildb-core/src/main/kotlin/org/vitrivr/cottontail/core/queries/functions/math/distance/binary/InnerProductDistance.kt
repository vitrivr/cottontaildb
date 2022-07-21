package org.vitrivr.cottontail.core.queries.functions.math.distance.binary

import jdk.incubator.vector.VectorOperators
import jdk.incubator.vector.VectorSpecies
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A [VectorDistance] implementation to calculate the inner product distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class InnerProductDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): VectorDistance<T>(type) {

    companion object: FunctionGenerator<DoubleValue> {
        val FUNCTION_NAME = Name.FunctionName("dotp")

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

    /** The [Cost] of applying this [InnerProductDistance]. */
    override val cost: Cost
        get() = ((Cost.FLOP * 3.0f + Cost.MEMORY_ACCESS * 2.0f) * this.d) + Cost.FLOP

    /**
     * [InnerProductDistance] for a [Complex64VectorValue].
     */
    class Complex64Vector(type: Types.Vector<Complex64VectorValue,*>): InnerProductDistance<Complex64VectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as Complex64VectorValue
            val query = arguments[1] as Complex64VectorValue
            var real = 0.0
            var imaginary = 0.0
            for (i in 0 until query.logicalSize) {
                val iprime = i shl 1
                real += query.data[iprime] * probing.data[iprime] + query.data[iprime + 1] * probing.data[iprime + 1]
                imaginary += query.data[iprime + 1] * probing.data[iprime] - query.data[iprime] * probing.data[iprime + 1]
            }
            return DoubleValue(1.0) - Complex64Value(real, imaginary).abs()
        }
        override fun copy(d: Int) = Complex64Vector(Types.Complex64Vector(d))

        override fun vectorized(): VectorDistance<Complex64VectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }

    /**
     * [InnerProductDistance] for a [Complex32VectorValue].
     */
    class Complex32Vector(type: Types.Vector<Complex32VectorValue,*>): InnerProductDistance<Complex32VectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as Complex32VectorValue
            val query = arguments[1] as Complex32VectorValue
            var real = 0.0
            var imaginary = 0.0
            for (i in 0 until query.logicalSize) {
                val iprime = i shl 1
                real += query.data[iprime] * probing.data[iprime] + query.data[iprime + 1] * probing.data[iprime + 1]
                imaginary += query.data[iprime + 1] * probing.data[iprime] - query.data[iprime] * probing.data[iprime + 1]
            }
            return Complex64Value(real, imaginary).abs()
        }
        override fun copy(d: Int) = Complex32Vector(Types.Complex32Vector(d))

        override fun vectorized(): VectorDistance<Complex32VectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }

    /**
     * [InnerProductDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,*>): InnerProductDistance<DoubleVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            var dotp = 0.0
            for (i in 0 until this.d) {
                dotp += query.data[i] * probing.data[i]
            }
            return DoubleValue(dotp)
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))

        override fun vectorized(): VectorDistance<DoubleVectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }

    /**
     * [InnerProductDistance] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,*>): InnerProductDistance<FloatVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var dotp = 0.0
            for (i in 0 until this.d) {
                dotp += (query.data[i] * probing.data[i])
            }
            return DoubleValue(dotp)
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))

        override fun vectorized(): VectorDistance<FloatVectorValue> {
            return FloatVectorVectorized(type)
        }
    }

    /**
     * SIMD Implementation: [InnerProductDistance] for a [FloatVectorValue].
     */
    class FloatVectorVectorized(type: Types.Vector<FloatVectorValue,*>): InnerProductDistance<FloatVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val species: VectorSpecies<Float> = jdk.incubator.vector.FloatVector.SPECIES_PREFERRED
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var vectorSum = jdk.incubator.vector.FloatVector.zero(species)

            //Vectorized calculation
            for (i in 0 until species.loopBound(this.d) step species.length()) {
                val vp = jdk.incubator.vector.FloatVector.fromArray(species, probing.data, i)
                val vq = jdk.incubator.vector.FloatVector.fromArray(species, query.data, i)
                vectorSum = vp.fma(vq, vectorSum)
            }

            var dotp = vectorSum.reduceLanes(VectorOperators.ADD)

            for (i in species.loopBound(this.d) until this.d) {
                dotp += (query.data[i] * probing.data[i])
            }

            return DoubleValue(dotp)
        }
        override fun copy(d: Int) = FloatVectorVectorized(Types.FloatVector(d))

        override fun vectorized(): VectorDistance<FloatVectorValue> {
            return this
        }
    }

    /**
     * [InnerProductDistance] for a [LongVectorValue].
     */
    class LongVector(type: Types.Vector<LongVectorValue,*>): InnerProductDistance<LongVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            val query = arguments[1] as LongVectorValue
            var dotp = 0.0
            for (i in 0 until this.d) {
                dotp += query.data[i] * probing.data[i]
            }
            return DoubleValue(dotp)
        }
        override fun copy(d: Int) = LongVector(Types.LongVector(d))

        override fun vectorized(): VectorDistance<LongVectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }

    /**
     * [InnerProductDistance] for a [IntVectorValue].
     */
    class IntVector(type: Types.Vector<IntVectorValue,*>): InnerProductDistance<IntVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            val query = arguments[1] as IntVectorValue
            var dotp = 0.0
            for (i in 0 until this.d) {
                dotp += (query.data[i] * probing.data[i])
            }
            return DoubleValue(dotp)
        }
        override fun copy(d: Int) = IntVector(Types.IntVector(d))

        override fun vectorized(): VectorDistance<IntVectorValue> {
            return this
            //TODO @Colin("Not yet implemented")
        }
    }
}