package org.vitrivr.cottontail.core.queries.functions.math.distance.binary

import jdk.incubator.vector.VectorOperators
import jdk.incubator.vector.VectorSpecies
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.*
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.*
import kotlin.math.pow
import kotlin.math.sqrt

/**
 * A [VectorDistance] implementation to calculate the Cosine distance between two [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class CosineDistance<T : VectorValue<*>>(type: Types.Vector<T,*>): VectorDistance<T>(type) {

    companion object: FunctionGenerator<DoubleValue> {
        val FUNCTION_NAME = Name.FunctionName("cosine")

        override val signature: Signature.Open
            get() = Signature.Open(FUNCTION_NAME, arrayOf(Argument.Vector, Argument.Vector))

        override fun obtain(signature: Signature.SemiClosed): Function<DoubleValue> {
            check(Companion.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${Companion.signature}. This is a programmer's error!"  }
            if (signature.arguments.any { it != signature.arguments[0] }) throw FunctionNotSupportedException("Function generator ${HaversineDistance.signature} cannot generate function with signature $signature.")
            return when(val type = signature.arguments[0].type) {
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
                DoubleVector(Types.DoubleVector(1)).signature,
                FloatVector(Types.FloatVector(1)).signature,
                LongVector(Types.LongVector(1)).signature,
                IntVector(Types.IntVector(1)).signature
            )
        }
    }

    /** The [Cost] of applying this [CosineDistance]. */
    override val cost: Cost
        get() = ((Cost.FLOP * 6.0f + Cost.MEMORY_ACCESS * 4.0f) * this.vectorSize) + Cost.FLOP * 4.0f + Cost.MEMORY_ACCESS * 3.0f

    /**
     * [CosineDistance] for a [DoubleVectorValue].
     */
    class DoubleVector(type: Types.Vector<DoubleVectorValue,*>): CosineDistance<DoubleVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as DoubleVectorValue
            val query = arguments[1] as DoubleVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in 0 until this.vectorSize) {
                dotp += (query.data[i] * probing.data[i])
                normq += query.data[i].pow(2)
                normv += probing.data[i].pow(2)
            }
            return DoubleValue(1.0 - (dotp / (sqrt(normq) * sqrt(normv))))
        }
        override fun copy(d: Int) = DoubleVector(Types.DoubleVector(d))
    }

    /**
     * [CosineDistance] for a [FloatVectorValue].
     */
    class FloatVector(type: Types.Vector<FloatVectorValue,*>): CosineDistance<FloatVectorValue>(type), VectorisableFunction<DoubleValue> {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in 0 until this.vectorSize) {
                dotp += (query.data[i] * probing.data[i])
                normq += query.data[i].pow(2)
                normv += probing.data[i].pow(2)
            }
            return DoubleValue(1.0 - (dotp / (sqrt(normq) * sqrt(normv))))
        }
        override fun copy(d: Int) = FloatVector(Types.FloatVector(d))
        override fun vectorized() =  FloatVectorVectorized(this.type)
    }

    /**
     * [CosineDistance] for a [FloatVectorValue].
     */
    class FloatVectorVectorized(type: Types.Vector<FloatVectorValue,*>): CosineDistance<FloatVectorValue>(type), VectorisedFunction<DoubleValue> {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val species: VectorSpecies<Float> = jdk.incubator.vector.FloatVector.SPECIES_PREFERRED
            val probing = arguments[0] as FloatVectorValue
            val query = arguments[1] as FloatVectorValue
            var vectorDotp = jdk.incubator.vector.FloatVector.zero(species)
            var vectorNormq = jdk.incubator.vector.FloatVector.zero(species)
            var vectorNormv = jdk.incubator.vector.FloatVector.zero(species)

            for (i in 0 until species.loopBound(this.vectorSize) step species.length()) {
                val vp = jdk.incubator.vector.FloatVector.fromArray(species, probing.data, i)
                val vq = jdk.incubator.vector.FloatVector.fromArray(species, query.data, i)

                vectorDotp = vp.fma(vq, vectorDotp)
                vectorNormq = vectorNormq.lanewise(VectorOperators.ADD, vq.pow(2f))
                vectorNormv = vectorNormv.lanewise(VectorOperators.ADD, vp.pow(2f))
            }

            var dotp = vectorDotp.reduceLanes(VectorOperators.ADD)
            var normq = vectorNormq.reduceLanes(VectorOperators.ADD)
            var normv = vectorNormv.reduceLanes(VectorOperators.ADD)

            for (i in species.loopBound(this.vectorSize) until this.vectorSize) {
                dotp += (query.data[i] * probing.data[i])
                normq += query.data[i].pow(2)
                normv += probing.data[i].pow(2)
            }

            return DoubleValue(dotp / (sqrt(normq) * sqrt(normv)))
        }
        override fun copy(d: Int) = FloatVectorVectorized(Types.FloatVector(d))
    }

    /**
     * [CosineDistance] for a [LongVectorValue].
     */
    class LongVector(type: Types.Vector<LongVectorValue,*>): CosineDistance<LongVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as LongVectorValue
            val query = arguments[1] as LongVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in 0 until this.vectorSize) {
                dotp += (query.data[i] * probing.data[i])
                normq += query.data[i].toDouble().pow(2)
                normv += probing.data[i].toDouble().pow(2)
            }
            return DoubleValue(1L - (dotp / (sqrt(normq) * sqrt(normv))))
        }
        override fun copy(d: Int) = LongVector(Types.LongVector(d))
    }

    /**
     * [CosineDistance] for a [IntVectorValue].
     */
    class IntVector(type: Types.Vector<IntVectorValue,*>): CosineDistance<IntVectorValue>(type) {
        override val name: Name.FunctionName = FUNCTION_NAME
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val probing = arguments[0] as IntVectorValue
            val query = arguments[1] as IntVectorValue
            var dotp = 0.0
            var normq = 0.0
            var normv = 0.0
            for (i in 0 until this.vectorSize) {
                dotp += (query.data[i] * probing.data[i])
                normq += query.data[i].toDouble().pow(2)
                normv += probing.data[i].toDouble().pow(2)
            }
            return DoubleValue(1 - (dotp / (sqrt(normq) * sqrt(normv))))
        }
        override fun copy(d: Int) = IntVector(Types.IntVector(d))
    }
}