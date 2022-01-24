package org.vitrivr.cottontail.functions.math.random

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.functions.*
import org.vitrivr.cottontail.core.functions.Function
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import java.util.*

/**
 * An [AbstractFunction] that generates and returns a random value.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class RandomFloatVector: AbstractFunction<FloatVectorValue>(Signature.Closed(Name.FunctionName("random_float_vector"), arrayOf(Argument.Typed(Types.Int)), Types.FloatVector(3))) {

    /** Internal [SplittableRandom] instance.*/
    private val random = SplittableRandom()

    /** Cost of executing this function */
    override val cost: Float = 1.0f

    companion object : AbstractFunctionGenerator<Value>() {
        override val signature: Signature.Open<out Value> = Signature.Open(Name.FunctionName("random_float_vector"), arrayOf(Argument.Typed(Types.Int)))
        override fun generateInternal(dst: Signature.Closed<*>): Function<Value> = RandomFloatVector()
    }

    /**
     * Generates and returns random value according to arguments.
     */
    override fun invoke(): FloatVectorValue =
        FloatVectorValue.random((this.arguments[0] as IntValue).value, this.random)
}