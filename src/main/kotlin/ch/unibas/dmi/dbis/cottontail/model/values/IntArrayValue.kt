package ch.unibas.dmi.dbis.cottontail.model.values

inline class IntArrayValue(override val value: IntArray) : Value<IntArray> {
    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("FloatArrayValue can can only be compared for equality.")
    }
}