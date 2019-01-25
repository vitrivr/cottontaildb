package ch.unibas.dmi.dbis.cottontail.database.entity

import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

/**
 * The header section of the [Entity] data structure.
 *
 * @see Entity
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class EntityHeader(var size: Long = 0, var created: Long = System.currentTimeMillis(), var modified: Long  = System.currentTimeMillis(), var columns: LongArray = LongArray(0), var indexes: LongArray = LongArray(0)) {
    companion object {
        /** The identifier that is used to identify a Cottontail DB [Entity] file. */
        internal const val IDENTIFIER: String = "COTTONT_ENT"

        /** The version of the Cottontail DB [Entity]  file. */
        internal const val VERSION: Short = 1
    }
}

/**
 * The [Serializer] for the [EntityHeader].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object EntityHeaderSerializer : Serializer<EntityHeader> {
    override fun serialize(out: DataOutput2, value: EntityHeader) {
        out.writeUTF(EntityHeader.IDENTIFIER)
        out.writeShort(EntityHeader.VERSION.toInt())
        out.packLong(value.size)
        out.writeLong(value.created)
        out.writeLong(value.modified)
        out.writeShort(value.columns.size)
        value.columns.forEach { out.packLong(it) }
        out.writeShort(value.indexes.size)
        value.indexes.forEach { out.packLong(it) }
    }

    override fun deserialize(input: DataInput2, available: Int): EntityHeader {
        if (!validate(input)) {
            throw DatabaseException.InvalidFileException("Cottontail DB Entity")
        }
        val size = input.unpackLong()
        val created = input.readLong()
        val modified = input.readLong()
        val columns = LongArray(input.readShort().toInt())
        for (i in 0 until columns.size) {
            columns[i] = input.unpackLong()
        }
        val indexes = LongArray(input.readShort().toInt())
        for (i in 0 until indexes.size) {
            indexes[i] = input.unpackLong()
        }
        return EntityHeader(size, created, modified, columns, indexes)
    }

    /**
     * Validates the [EntityHeader]. Must be executed before deserialization
     *
     * @return True if validation was successful, false otherwise.
     */
    private fun validate(input: DataInput2): Boolean {
        val identifier = input.readUTF()
        val version = input.readShort()
        return (version == EntityHeader.VERSION) and (identifier == EntityHeader.IDENTIFIER)
    }
}