package org.vitrivr.cottontail.dbms.column.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.general.DBOVersion

/**
 * The header data structure of any [MapDBColumn]
 *
 * @see MapDBColumn
 * @author Ralph Gasser
 * @version 2.0.1
 */
data class ColumnHeader(
    val columnDef: ColumnDef<*>,
    val count: Long = 0L,
    val created: Long = System.currentTimeMillis(),
    val modified: Long = System.currentTimeMillis()
) {
    companion object Serializer : org.mapdb.Serializer<ColumnHeader> {
        override fun serialize(out: DataOutput2, value: ColumnHeader) {
            out.packInt(DBOVersion.V2_0.ordinal)
            out.writeUTF(value.columnDef.name.toString())
            out.packInt(value.columnDef.type.ordinal)
            out.packInt(value.columnDef.type.logicalSize)
            out.writeBoolean(value.columnDef.nullable)
            out.writeBoolean(value.columnDef.primary)
            out.writeLong(value.count)
            out.writeLong(value.created)
            out.writeLong(value.modified)
        }

        override fun deserialize(input: DataInput2, available: Int): ColumnHeader {
            val version = DBOVersion.values()[input.unpackInt()]
            if (version != DBOVersion.V2_0) throw DatabaseException.VersionMismatchException(version, DBOVersion.V2_0)
            val name = Name.ColumnName(*input.readUTF().split('.').toTypedArray())
            val def = ColumnDef(name, Types.forOrdinal(input.unpackInt(), input.unpackInt()), input.readBoolean(), input.readBoolean())
            return ColumnHeader(def, input.readLong(), input.readLong(), input.readLong())
        }
    }
}