package org.vitrivr.cottontail.functions.math.distance.basics

import org.vitrivr.cottontail.functions.basics.Argument
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.functions.basics.Signature
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue

/**
 * A kernel function used for distance calculations between a query [VectorValue] and other vector values.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
interface VectorDistance<T : VectorValue<*>>: Function.Stateful<DoubleValue> {

    /** The [Name.FunctionName] of this [VectorDistance]. */
    val name: Name.FunctionName

    /** The [Type] accepted by this [VectorDistance] [Function]. Must be a [VectorValue] type. */
    val type: Type<out T>

    /** The dimensionality of this [VectorDistance]. */
    val d: Int
        get() = this.type.logicalSize

    /**
     * Creates a copy of this [VectorDistance]. Can be used to create different shapes by choosing a new value for [d].
     *
     * @param d The new dimensionality of the [VectorDistance].
     * @return New [VectorDistance]
     */
    fun copy(d: Int = this.d): VectorDistance<T>

    /**
     * A [Binary] vector distance, taking two arguments (a query (-vector) and a vector to compare with).
     *
     * This is pretty much the default [VectorDistance] used in NNS.
     */
    interface Binary<T : VectorValue<*>>: VectorDistance<T> {
        /** The [Signature.Closed] of this [VectorDistance.Binary] [Function]. */
        override val signature: Signature.Closed<out DoubleValue>
            get() = Signature.Closed(this.name, arrayOf(Argument.Typed(this.type), Argument.Typed(this.type)), Type.Double)

        /** By convention, the argument at position 1 (query argument) is stateful for [Binary]. */
        override val statefulArguments: IntArray
            get() = intArrayOf(1)

        /** The query [VectorValue]. */
        val query: T
    }
}