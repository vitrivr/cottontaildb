package ch.unibas.dmi.dbis.cottontail.model.values

/**
 * A [Regex] value. Only used during query execution; cannot be stored.
 *
 * @author Ralph Gasser
 * @param 1.0
 */
inline class RegexValue(override val value: Regex) : Value<Regex> {
    override val size: Int
        get() = 0

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("RegexValue can only be compared to other StringValues.")
    }
}
