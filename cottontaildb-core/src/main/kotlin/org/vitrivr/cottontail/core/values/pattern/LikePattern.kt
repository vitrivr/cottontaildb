package org.vitrivr.cottontail.core.values.pattern

import org.vitrivr.cottontail.core.values.StringValue

/**
 * A [Pattern] that corresponds to a LIKE expression, i.e., uses SQL wildcards. It can either
 * be converted to a [LucenePattern] or matched directly through regular expressions.
 *
 * @author Ralph Gasser
 * @version 1.1.2
 */
sealed class LikePattern(pattern: String) : Pattern(pattern) {

    companion object {
        /** Wildcard used to match zero, one or multiple characters. */
        const val ZERO_ONE_MULTIPLE_WILDCARD = '%'

        /** Wildcard used to match one character. */
        const val SINGLE_WILDCARD = '_'

        /**
         * Converts an expression to a [LikePattern].
         *
         * @param exp The expression to convert.
         * @return [LikePattern]
         */
        fun forValue(exp: String) = when {
            exp.endsWith(ZERO_ONE_MULTIPLE_WILDCARD) && exp.count { it == ZERO_ONE_MULTIPLE_WILDCARD } == 1 ||
            exp.endsWith(SINGLE_WILDCARD) && exp.count { it == SINGLE_WILDCARD } == 1 -> StartsWith(exp)
            exp.startsWith(ZERO_ONE_MULTIPLE_WILDCARD) && exp.count { it == ZERO_ONE_MULTIPLE_WILDCARD } == 1 ||
            exp.startsWith(SINGLE_WILDCARD) && exp.count { it == SINGLE_WILDCARD } == 1 -> EndsWith(exp)
            else -> Regex(exp)
        }
    }

    /**
     * A [LikePattern] for LIKE patterns that can be expressed with a regular expression.
     */
    class Regex(value: String) : LikePattern(value) {
        /** The [Regex] expression used for matching; created lazily */
        private val regex by lazy {
            val cleaned = value.replace("\\", "\\\\")
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
                .replace("$ZERO_ONE_MULTIPLE_WILDCARD", ".*") /* LIKE syntax to match multiple characters: % --> .* */
                .replace("$SINGLE_WILDCARD", ".?") /* LIKE syntax to match single character: _ --> .? TODO: This can be optimized. I.e. ___ === .{3} */
            Regex(cleaned, RegexOption.DOT_MATCHES_ALL)
        }

        /**
         * Checks if the given [StringValue] matches this [LikePattern].
         *
         * @param value [StringValue] to match.
         * @return True on match, false otherwise.
         */
        override fun matches(value: StringValue) = this.regex.matches(value.value)
    }

    /**
     * A [LikePattern] for LIKE patterns that are equivalent to a 'startsWith' semantic, i.e., LIKE 'XYZ%'.
     *
     * Supposed to be faster than [LikePattern.Regex]
     */
    class StartsWith(value: String) : LikePattern(value) {

        init {
            val wildcardCount = this.pattern.count { it == ZERO_ONE_MULTIPLE_WILDCARD || it == SINGLE_WILDCARD }
            require(this.pattern[this.pattern.length - 1] == ZERO_ONE_MULTIPLE_WILDCARD && wildcardCount == 1) {
                "StartsWith LIKE patterns must only contain a single wildcard % at the end."
            }
        }

        /** String used for comparison. */
        val startsWith = this.pattern.subSequence(0, value.length - 1)

        /**
         * Checks if the given [StringValue] matches this [LikePattern].
         *
         * @param value [StringValue] to match.
         * @return True on match, false otherwise.
         */
        override fun matches(value: StringValue): Boolean = value.value.startsWith(this.startsWith)
    }

    /**
     * A [LikePattern] for LIKE patterns that are equivalent to a 'endsWith' semantic, i.e., LIKE '%XYZ'.
     *
     * Supposed to be faster than [LikePattern.Regex]
     */
    class EndsWith(value: String) : LikePattern(value) {

        init {
            val wildcardCount = this.pattern.count { it == ZERO_ONE_MULTIPLE_WILDCARD || it == SINGLE_WILDCARD }
            require(this.pattern[0] == ZERO_ONE_MULTIPLE_WILDCARD && wildcardCount == 1) { "EndsWith LIKE patterns must only contain a single wildcard % at the beginning." }
        }

        /** String used for comparison. */
        val endsWith = this.pattern.substring(1, value.length)

        /**
         * Checks if the given [StringValue] matches this [LikePattern].
         *
         * @param value [StringValue] to match.
         * @return True on match, false otherwise.
         */
        override fun matches(value: StringValue) = value.value.endsWith(this.endsWith)
    }

    /**
     * Converts this [LikePattern] to a [LucenePattern] and returns it.
     *
     * @return [LucenePattern] that corresponds to this [LikePattern]
     */
    fun toLucene(): LucenePattern {
        val cleaned = this.pattern
            .replace(ZERO_ONE_MULTIPLE_WILDCARD, '*') /* LIKE syntax to match multiple characters: % --> * */
            .replace(SINGLE_WILDCARD, '?') /* LIKE syntax to match single character: _ --> ? */
        return LucenePattern(cleaned)
    }
}