package org.vitrivr.cottontail.core.queries.functions.math.distance.binary

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.DoubleValue

/**
 * A binary [Function] used for distance calculations between a query [VectorValue] and other vector values.
 *
 * These type of [Function] always accept two of the same [Types.Vector] (query argument and probing argument)
 * and always return a [Types.Double].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class VectorDistance<T: VectorValue<*>>(val type: Types.Vector<T,*>): Function<DoubleValue> {

    /** The [Types.Vector] accepted by this [VectorDistance]. */
    abstract val name: Name.FunctionName

    /** The dimensionality of this [VectorDistance]. */
    val vectorSize: Int
        get() = this.type.logicalSize

    /** Signature of a [VectorDistance] is defined by the argument type it accepts. */
    override val signature: Signature.Closed<DoubleValue>
        get() = Signature.Closed(name, arrayOf(this.type, this.type), Types.Double)


    /** */
    open fun invokeOrMaximum(left: VectorValue<*>, right: VectorValue<*>, maximum: DoubleValue): DoubleValue? = this.invoke(left, right)

    /**
     * Creates a copy of this [VectorDistance].
     *
     * @return Copy of this [VectorDistance]
     */
    override fun copy(): VectorDistance<T> = copy(this.vectorSize)

    /**
     * Creates a reshaped copy of this [VectorDistance].
     *
     * @return Copy of this [VectorDistance]
     */
    abstract fun copy(d: Int): VectorDistance<T>

    override fun equals(other: Any?): Boolean {
        if (other !is VectorDistance<*>) return false
        if (other.name != this.name) return false
        if (other.type != this.type) return false
        return true
    }


    override fun hashCode(): Int {
        val result = this.name.hashCode()
        return 31 * result + this.type.hashCode()
    }
}