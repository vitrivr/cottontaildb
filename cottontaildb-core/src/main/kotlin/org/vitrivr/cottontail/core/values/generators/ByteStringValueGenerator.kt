package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.values.ByteStringValue
import java.util.random.RandomGenerator

object ByteStringValueGenerator : ValueGenerator<ByteStringValue> {

    override fun random(rnd: RandomGenerator): ByteStringValue = ByteStringValue(ByteArray(rnd.nextInt(4096)){ rnd.nextInt().toByte() })

    fun empty() = ByteStringValue.EMPTY
}