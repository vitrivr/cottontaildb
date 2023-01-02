package org.vitrivr.cottontail.core.values.generators

import org.apache.commons.math3.random.RandomGenerator
import org.vitrivr.cottontail.core.values.ByteStringValue

object ByteStringValueGenerator : ValueGenerator<ByteStringValue> {

    override fun random(rnd: RandomGenerator): ByteStringValue = ByteStringValue(ByteArray(rnd.nextInt(4096)){ rnd.nextInt().toByte() })

    fun empty() = ByteStringValue.EMPTY

}