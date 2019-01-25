package ch.unibas.dmi.dbis.cottontail.database.column

/**
 * A definition class for a [Column]. Specifies all the properties of such [Column].
 *
 * @see Column
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class ColumnDef(val name: String, val type: ColumnType<*>, val size: Int = 1, val nullable: Boolean = true) {
    companion object {
        /**
         * Returns a [ColumnDef] with the provided attributes. The only difference as compared to using the constructor,
         * is that the [ColumnType] can be provided by name.
         *
         * @param column Name of the new [Column]
         * @param type Name of the [ColumnType] of the new [Column]
         * @param size Size of the new [Column] (e.g. for vectors), where eligible.
         * @param nullable Whether or not the [Column] should be nullable.
         */
        fun withAttributes(column: String, type: String, size: Int = -1, nullable: Boolean = true): ColumnDef = ColumnDef(column, ColumnType.forName(type), size, nullable)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as ColumnDef

        if (name != other.name) return false
        if (type != other.type) return false

        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + type.hashCode()
        return result
    }
}