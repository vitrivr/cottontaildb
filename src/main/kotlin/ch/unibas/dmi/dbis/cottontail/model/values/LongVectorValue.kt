package ch.unibas.dmi.dbis.cottontail.model.values

inline class LongVectorValue(override val value: LongArray) : Value<LongArray> {
    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("FloatArrayValue can can only be compared for equality.")
    }
}