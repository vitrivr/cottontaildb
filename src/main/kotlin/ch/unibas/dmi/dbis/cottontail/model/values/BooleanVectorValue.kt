package ch.unibas.dmi.dbis.cottontail.model.values

import java.util.*

inline class BooleanVectorValue(override val value: BitSet) : Value<BitSet> {
    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("BitSet can can only be compared for equality.")
    }
}