package ch.unibas.dmi.dbis.cottontail.database.schema

class ColumnDef(val name: String, val type: ColumnType<*>, val size: Int = 1, val nullable: Boolean = true) {
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