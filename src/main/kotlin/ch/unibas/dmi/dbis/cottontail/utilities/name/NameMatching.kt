package ch.unibas.dmi.dbis.cottontail.utilities.name

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.model.exceptions.QueryException

/** The separator between name components. */
const val COTTONTAIL_NAME_COMPONENT_SEPARATOR = '.'

/**
 * Checks if the provided [String is a prefix of this [String] in terms of Cottontail DB naming.
 *
 * @param other The [String] to check.
 */
internal fun String.isPrefix(other: String): Boolean {
    val o = other.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR)
    val t = this.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR)
    return if (o.size > t.size)
        false
    else
        o.mapIndexed {i,s -> s == t[i]}.all { it }

}

/**
 * Checks if the provided [String] matches this [String] in terms of Cottontail DB naming.
 *
 * @param other The [String] to check.
 */
internal fun String.doesNameMatch(other: String): Boolean {
    val o = other.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR)
    val t = this.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR)
    return if (o.size != t.size)
        false
    else
        o.mapIndexed {i,s -> if (i<o.size-1) {s == t[i]} else {s == t[i] || s == "*"}}.all { it }
}

/**
 * Normalizes this [String] to a column name relative to the given [Entity]. Column names always consist
 * of four components: <instance>.<schema>.<entity>.<column>
 *
 * <strong>Important:</strong> This method does not check, whether the specified column actually exists!
 *
 * @param entity [Entity] relative to which the name should be normalized.
 */
internal fun String.normalizeColumnName(entity: Entity): String {
    val split = this.split(COTTONTAIL_NAME_COMPONENT_SEPARATOR)
    return when (split.size) {
        1 -> "${entity.fqn}.${split[0]}"
        2 -> "${entity.parent.fqn}.${split[0]}.${split[1]}"
        3 -> "${entity.parent.parent.fqn}.${split[0]}.${split[1]}.${split[2]}"
        4 -> "${split[0]}.${split[1]}.${split[2]}.${split[3]}"
        else -> throw QueryException.QueryBindException("The provided expression '$this' does not point to a column.")
    }
}