package org.vitrivr.cottontail.functions.math.distance.binary

import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.functions.basics.AbstractFunctionGenerator
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.functions.exception.FunctionNotSupportedException
import org.vitrivr.cottontail.functions.math.distance.VectorDistance
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import kotlin.math.*

/**
 * A [VectorDistance] implementation to calculate the Haversine distance between two 2D points.
 *
 * @author Loris Sauter & Ralph Gasser
 * @version 1.1.0
 */
sealed class HaversineDistance<T : VectorValue<*>>: VectorDistance<T> {

    object Generator: AbstractFunctionGenerator<DoubleValue>() {
        val FUNCTION_NAME = Name.FunctionName("haversine")

        const val RADIUS_EARTH = 6371000.0

        override val signature: Signature.Open<out DoubleValue>
            get() = Signature.Open(FUNCTION_NAME, arity = 2, Type.Double)

        override fun generateInternal(vararg arguments: Type<*>): Function.Dynamic<DoubleValue> = when {
            arguments[0] is Type.DoubleVector && arguments[0].logicalSize == 2 -> CosineDistance.DoubleVector(2)
            arguments[0] is Type.FloatVector && arguments[0].logicalSize == 2 -> CosineDistance.FloatVector(2)
            arguments[0] is Type.LongVector && arguments[0].logicalSize == 2 -> CosineDistance.LongVector(2)
            arguments[0] is Type.IntVector && arguments[0].logicalSize == 2 -> CosineDistance.IntVector(2)
            else -> throw FunctionNotSupportedException(this.signature)
        }
    }

    /** Name of this [HaversineDistance]. */
    override val name: Name.FunctionName = Generator.FUNCTION_NAME

    /** The cost of applying this [HaversineDistance] to a single [VectorValue]. */
    override val cost: Float
        get() = this.d * 2.0f * Cost.COST_MEMORY_ACCESS + 27.0f * Cost.COST_FLOP + 20.0f * Cost.COST_MEMORY_ACCESS

    /**
     * Calculates the haversine distance between two points a, b.
     *
     * @param aLat The latitude of a.
     * @param aLon The longitude of a.
     * @param bLat The latitude of b.
     * @param bLon The longitude of b.
     */
    protected fun haversine(aLat: Double, aLon: Double, bLat: Double, bLon: Double): Double {
        val phi1 = StrictMath.toRadians(aLat)
        val phi2 = StrictMath.toRadians(bLat)
        val deltaPhi = StrictMath.toRadians(bLat - aLat)
        val deltaLambda = StrictMath.toRadians(bLon - aLon)
        val c = sin(deltaPhi / 2.0) * sin(deltaPhi / 2.0) + cos(phi1) * cos(phi2) * sin(deltaLambda / 2.0) * sin(deltaLambda / 2.0)
        val d = 2.0 * atan2(sqrt(c), sqrt(1 - c))
        return Generator.RADIUS_EARTH * d
    }

    /**
     * [HaversineDistance] for a [DoubleVectorValue].
     */
    class DoubleVector: HaversineDistance<DoubleVectorValue>() {
        override val type = Type.DoubleVector(2)
        override fun copy(d: Int) = DoubleVector()
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as DoubleVectorValue
            val vector = arguments[0] as DoubleVectorValue
            return DoubleValue(haversine(query.data[0], query.data[1], vector.data[0], vector.data[1]))
        }
    }

    /**
     * [HaversineDistance] for a [FloatVectorValue].
     */
    class FloatVector: HaversineDistance<FloatVectorValue>() {
        override val type = Type.FloatVector(2)
        override fun copy(d: Int) = FloatVector()
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as FloatVectorValue
            val vector = arguments[0] as FloatVectorValue
            return DoubleValue(haversine(query.data[0].toDouble(), query.data[1].toDouble(), vector.data[0].toDouble(), vector.data[1].toDouble()))
        }
    }

    /**
     * [HaversineDistance] for a [LongVectorValue].
     */
    class LongVector: HaversineDistance<LongVectorValue>() {
        override val type = Type.LongVector(2)
        override fun copy(d: Int) = LongVector()
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as LongVectorValue
            val vector = arguments[0] as LongVectorValue
            return DoubleValue(haversine(query.data[0].toDouble(), query.data[1].toDouble(), vector.data[0].toDouble(), vector.data[1].toDouble()))
        }
    }

    /**
     * [HaversineDistance] for a [IntVectorValue].
     */
    class IntVector: HaversineDistance<IntVectorValue>() {
        override val type = Type.IntVector(2)
        override fun copy(d: Int) = IntVector()
        override fun invoke(vararg arguments: Value?): DoubleValue {
            val query = arguments[0] as IntVectorValue
            val vector = arguments[1] as IntVectorValue
            return DoubleValue(haversine(query.data[0].toDouble(), query.data[1].toDouble(), vector.data[0].toDouble(), vector.data[1].toDouble()))
        }
    }
}