package ch.unibas.dmi.dbis.cottontail.database.queries

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef

data class Projection(val type: ProjectionType = ProjectionType.SELECT, val columns: Array<ColumnDef<*>>) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Projection

        if (type != other.type) return false
        if (!columns.contentEquals(other.columns)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + columns.contentHashCode()
        return result
    }
}

/**
 *
 */
enum class ProjectionType {
    SELECT,
    COUNT,
    EXISTS
}