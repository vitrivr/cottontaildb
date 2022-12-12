package org.vitrivr.cottontail.legacy.v2.column

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.general.DBOVersion

/**
 * The header data structure of any [ColumnV2]
 *
 * @see ColumnV2
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class ColumnV2Header(
    val columnDef: ColumnDef<*>,
    val count: Long = 0L,
    val created: Long = System.currentTimeMillis(),
    val modified: Long = System.currentTimeMillis()
) {
    companion object Serializer : org.mapdb.Serializer<ColumnV2Header> {
        override fun serialize(out: DataOutput2, value: ColumnV2Header) {
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

        override fun deserialize(input: DataInput2, available: Int): ColumnV2Header {
            val version = DBOVersion.values()[input.unpackInt()]
            if (version != DBOVersion.V2_0)
                throw DatabaseException.VersionMismatchException(version, DBOVersion.V2_0)
            val names = input.readUTF().split('.').toTypedArray()
            val def = ColumnDef(
                Name.ColumnName.create(names[1], names[2], names[3]),
                Types.forOrdinal(input.unpackInt(), input.unpackInt()),
                input.readBoolean(),
                input.readBoolean()
            )
            return ColumnV2Header(def, input.readLong(), input.readLong(), input.readLong())
        }
    }
}