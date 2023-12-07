package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.values.DoubleVectorValue
import java.util.random.RandomGenerator

/**
 * A [VectorValueGenerator] for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DoubleVectorValueGenerator: VectorValueGenerator<DoubleVectorValue> {
    override fun random(size: Int, rnd: RandomGenerator) = DoubleVectorValue(DoubleArray(size) { rnd.nextDouble() })
    override fun one(size: Int) = DoubleVectorValue(DoubleArray(size) { 1.0 })
    override fun zero(size: Int) = DoubleVectorValue(DoubleArray(size))
    override fun with(values: Array<Number>) = DoubleVectorValue(values)
}