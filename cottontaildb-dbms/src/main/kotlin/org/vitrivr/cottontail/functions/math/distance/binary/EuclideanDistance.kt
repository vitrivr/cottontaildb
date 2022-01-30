package org.vitrivr.cottontail.functions.math.distance.binary

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.core.queries.functions.math.MinkowskiDistance
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.FunctionGenerator
import org.vitrivr.cottontail.core.queries.functions.Signature
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
            check(this.signature.collides(signature)) { "Provided signature $signature is incompatible with generator signature ${this.signature}. This is a programmer's error!"  }
            return when(val type = signature.arguments[0].type) {
                is Types.Complex64Vector -> Complex64Vector(type)
                is Types.Complex32Vector -> Complex32Vector(type)
                is Types.DoubleVector -> DoubleVector(type)
                is Types.FloatVector -> FloatVector(type)
                is Types.LongVector -> LongVector(type)
                is Types.IntVector -> IntVector(type)
                else -> throw FunctionNotSupportedException("Function generator ${this.signature} cannot generate function with signature $signature.")
            }
        }

        override fun resolve(signature: Signature.Open): List<Signature.Closed<*>> {
            if (this.signature != signature) throw FunctionNotSupportedException("Function generator ${this.signature} cannot generate function with signature $signature.")
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

    /** The cost of applying this [EuclideanDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = this.d * (3.0f * Cost.COST_FLOP + 2.0f * Cost.COST_MEMORY_ACCESS) + Cost.COST_FLOP + Cost.COST_MEMORY_ACCESS


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
            for (i in 0 until this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2)
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
            val probing = arguments[0] as Complex32VectorValue
            val query = arguments[1] as Complex32VectorValue
            var sum = 0.0
            for (i in 0 until this.d) {
                sum += (query.data[i] - probing.data[i]).pow(2)
            }
            return DoubleValue(sqrt(sum))
        }
        override fun copy(d: Int) = Complex32Vector(Types.Complex32Vector(d))
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
    }
}