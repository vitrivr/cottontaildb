package ch.unibas.dmi.dbis.cottontail.model.values

inline class DoubleArrayValue(override val value: DoubleArray) : Value<DoubleArray> {
    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("DoubleArrayValue can can only be compared for equality.")
    }
}