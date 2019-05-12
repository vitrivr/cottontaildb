package ch.unibas.dmi.dbis.cottontail.model.values

inline class BooleanArrayValue(override val value: BooleanArray) : Value<BooleanArray> {
    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("BooleanArrayValue can can only be compared for equality.")
    }
}