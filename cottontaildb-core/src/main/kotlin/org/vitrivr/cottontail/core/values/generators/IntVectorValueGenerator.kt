package org.vitrivr.cottontail.core.values.generators

import org.apache.commons.math3.random.RandomGenerator
import org.vitrivr.cottontail.core.values.IntVectorValue

/**
 * A [VectorValueGenerator] for [IntVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object IntVectorValueGenerator: VectorValueGenerator<IntVectorValue> {
    override fun random(size: Int, rnd: RandomGenerator) = IntVectorValue(IntArray(size) { rnd.nextInt() })
    override fun one(size: Int) = IntVectorValue(IntArray(size) { 1 })
    override fun zero(size: Int) = IntVectorValue(IntArray(size))
    override fun with(values: Array<Number>) = IntVectorValue(values)
}