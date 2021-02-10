package org.vitrivr.cottontail.database.index

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.model.exceptions.DatabaseException

/**
 * The header section of an [Index] data structure.
 *
 * @see Index
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class IndexHeader(
    val name: String,
    val type: IndexType,
    val columns: Array<ColumnDef<*>>,
    val created: Long = System.currentTimeMillis(), val modified: Long = System.currentTimeMillis()
) {

    companion object Serializer : org.mapdb.Serializer<IndexHeader> {
        /** The version of the Cottontail DB [Entity]  file. */
        const val VERSION: Short = 2

        override fun serialize(out: DataOutput2, value: IndexHeader) {
            out.writeShort(VERSION.toInt())
            out.writeUTF(value.name)
            out.packInt(value.type.ordinal)
            out.packInt(value.columns.size)
            value.columns.forEach { ColumnDef.serialize(out, it) }
        }

        override fun deserialize(input: DataInput2, available: Int): IndexHeader {
            val version = input.readShort()
            if (version != VERSION) throw DatabaseException.VersionMismatchException(
                version,
                VERSION
            )
            return IndexHeader(
                input.readUTF(),
                IndexType.values()[input.unpackInt()],
                Array(input.unpackInt()) { ColumnDef.deserialize(input, available) }
            )
        }
    }
}