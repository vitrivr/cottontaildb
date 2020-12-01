package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.ScalarValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [PatternValue] value as used in LIKE queries. Suppoers '*' and '_' wildcard for matching multiple
 * and single characters. Only used during query execution; cannot be stored!
 *
 * @author Ralph Gasser
 * @param 1.1.3
 */
class PatternValue(override val value: String) : ScalarValue<String> {

    /**
     * Returns a [Regex] representation of this [PatternValue].
     */
    private val regex by lazy {
        val converted = this@PatternValue.value.replace("\\", "\\\\")
                .replace("[", "\\[") /* Escape. */
                .replace("]", "\\]") /* Escape. */
                .replace("(", "\\(") /* Escape. */
                .replace(")", "\\)") /* Escape. */
                .replace("{", "\\{") /* Escape. */
                .replace("}", "\\}") /* Escape. */
                .replace("^", "\\^") /* Escape. */
                .replace("$", "\\$") /* Escape. */
                .replace(".", "\\.") /* Escape. */
                .replace("?", "\\?") /* Escape. */
                .replace("+", "\\+") /* Escape. */
                .replace("-", "\\-") /* Escape. */
                .replace("|", "\\|") /* Escape. */
                .replace("*", ".*") /* LIKE syntax to match multiple characters: * --> .* */
                .replace("_", ".?") /* LIKE syntax to match single character: _ --> .? */
        Regex(converted)
    }

    /**
     * Returns a value representation of this [PatternValue] that can be understood by Apache Lucene.
     */
    val lucene by lazy {
        this@PatternValue.value
    }

    override val logicalSize: Int
        get() = 0

    /**
     * Checks if the given [StringValue] matches this [PatternValue].
     *
     * @param match [StringValue]
     * @return True on match, false otherwise,
     */
    fun matches(match: StringValue) = this.regex.matches(match.value)

    override fun compareTo(other: Value): Int = when (other) {
        is PatternValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("PatternValues can only be compared to other PatternValues.")
    }

    override fun isEqual(other: Value): Boolean = (other is PatternValue) && (other.value == this.value)
}
