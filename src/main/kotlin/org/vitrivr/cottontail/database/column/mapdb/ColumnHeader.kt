package org.vitrivr.cottontail.database.column.mapdb

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header data structure of any [MapDBColumn]
 *
 * @see MapDBColumn
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class ColumnHeader(
    val columnDef: ColumnDef<*>,
    val count: Long = 0L,
    val created: Long = System.currentTimeMillis(),
    val modified: Long = System.currentTimeMillis()
) {
    companion object Serializer : org.mapdb.Serializer<ColumnHeader> {
        /** The version of the Cottontail DB [MapDBColumn] file. */
        const val VERSION: Short = 2

        override fun serialize(out: DataOutput2, value: ColumnHeader) {
            out.writeShort(VERSION.toInt())
            ColumnDef.serialize(out, value.columnDef)
            out.writeLong(value.count)
            out.writeLong(value.created)
            out.writeLong(value.modified)
        }

        override fun deserialize(input: DataInput2, available: Int): ColumnHeader {
            val version = input.readShort()
            if (version != VERSION) throw DatabaseException.VersionMismatchException(
                version,
                VERSION
            )
            val def = ColumnDef.deserialize(input, available)
            return ColumnHeader(def, input.readLong(), input.readLong(), input.readLong())
        }
    }
}