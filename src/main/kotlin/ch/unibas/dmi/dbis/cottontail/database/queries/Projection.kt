package ch.unibas.dmi.dbis.cottontail.database.queries

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef

/**
 * Formalizes a [Projection] operation in the Cottontail query execution engine.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
data class Projection(val type: ProjectionType = ProjectionType.SELECT, val columns: Array<ColumnDef<*>>, val fields: Map<String,String?>) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Projection

        if (type != other.type) return false
        if (!columns.contentEquals(other.columns)) return false
        if (fields != other.fields) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + columns.contentHashCode()
        result = 31 * result + fields.hashCode()
        return result
    }
}

/**
 * The type [Projection]
 */
enum class ProjectionType {
    SELECT,
    COUNT,
    EXISTS
}