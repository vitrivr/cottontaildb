package ch.unibas.dmi.dbis.cottontail.model.values

interface Value<T> {
    val value: T
    val size
        get() = when (this.value) {
            is DoubleArray -> (value as DoubleArray).size
            is FloatArray -> (value as FloatArray).size
            is LongArray -> (value as LongArray).size
            is IntArray -> (value as IntArray).size
            is BooleanArray -> (value as BooleanArray).size
            else -> -1
        }
}