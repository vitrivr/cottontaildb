package org.vitrivr.cottontail.model.values

inline class StringValue(override val value: String) : Value<String> {
    override val size: Int
        get() = value.length

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int = when (other) {
        is StringValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("StringValues can only be compared to other StringValues.")
    }
}