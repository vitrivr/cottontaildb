package org.vitrivr.cottontail.database.entity

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.column.ColumnDriver
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.nio.file.Path
import java.nio.file.Paths

/**
 * The header section of the [Entity] data structure.
 *
 * @see Entity
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class EntityHeader(
    var name: String,
    var created: Long = System.currentTimeMillis(),
    var modified: Long = System.currentTimeMillis(),
    var columns: List<ColumnRef> = emptyList(),
    var indexes: List<IndexRef> = emptyList()
) {
    companion object Serializer : org.mapdb.Serializer<EntityHeader> {
        /** The version of the Cottontail DB [Entity]  file. */
        const val VERSION: Short = 2

        override fun serialize(out: DataOutput2, value: EntityHeader) {
            out.writeShort(VERSION.toInt())
            out.writeUTF(value.name)
            out.writeLong(value.created)
            out.writeLong(value.modified)
            out.packInt(value.columns.size)
            value.columns.forEach { ColumnRef.serialize(out, it) }
            out.packInt(value.indexes.size)
            value.indexes.forEach { IndexRef.serialize(out, it) }
        }

        override fun deserialize(input: DataInput2, available: Int): EntityHeader {
            val version = input.readShort()
            if (version != VERSION) throw DatabaseException.VersionMismatchException(
                version,
                VERSION
            )
            return EntityHeader(
                input.readUTF(),
                input.readLong(),
                input.readLong(),
                (0 until input.unpackInt()).map { ColumnRef.deserialize(input, available) },
                (0 until input.unpackInt()).map { IndexRef.deserialize(input, available) }
            )
        }
    }

    /**
     * Reference pointing to a column.
     */
    data class ColumnRef(val name: String, val type: ColumnDriver, val path: Path) {
        companion object Serializer : org.mapdb.Serializer<ColumnRef> {
            override fun serialize(out: DataOutput2, value: ColumnRef) {
                out.writeUTF(value.name)
                out.packInt(value.type.ordinal)
                out.writeUTF(value.path.toString())
            }

            override fun deserialize(input: DataInput2, available: Int): ColumnRef = ColumnRef(
                input.readUTF(),
                ColumnDriver.values()[input.unpackInt()],
                Paths.get(input.readUTF())
            )
        }
    }

    /**
     * Reference pointing to an index.
     */
    data class IndexRef(val name: String, val type: IndexType, val path: Path) {
        companion object Serializer : org.mapdb.Serializer<IndexRef> {
            override fun serialize(out: DataOutput2, value: IndexRef) {
                out.writeUTF(value.name)
                out.packInt(value.type.ordinal)
                out.writeUTF(value.path.toString())
            }

            override fun deserialize(input: DataInput2, available: Int): IndexRef = IndexRef(
                input.readUTF(),
                IndexType.values()[input.unpackInt()],
                Paths.get(input.readUTF())
            )
        }
    }
}