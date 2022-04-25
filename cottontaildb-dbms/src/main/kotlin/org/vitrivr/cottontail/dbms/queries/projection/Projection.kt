package org.vitrivr.cottontail.dbms.queries.projection

/**
 * Enumeration of all [Projection] operations supported by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
enum class Projection(val aggregating: Boolean) {
    SELECT(false),
    SELECT_DISTINCT(false),
    COUNT(true),
    COUNT_DISTINCT(true),
    EXISTS(true),
    SUM(true),
    MAX(true),
    MIN(true),
    MEAN(true);

    /**
     * Converts this [Projection] to a [String] labels.
     *
     * @return String label.
     */
    fun label() = this.toString().replace("_", "").lowercase().replaceFirstChar {
        it.uppercase()
    }

    /**
     * Converts this [Projection] to a [String] column name.
     *
     * @return String label.
     */
    fun column() = this.toString().replace("_", "").lowercase()
}