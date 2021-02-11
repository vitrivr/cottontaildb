package org.vitrivr.cottontail.database.schema

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.database.entity.EntityHeader
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

/**
 * The header of the [DefaultSchema]. Contains information regarding its content!
 *
 * @see DefaultSchema
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
data class SchemaHeader(
    val name: String,
    val created: Long = System.currentTimeMillis(),
    var modified: Long = System.currentTimeMillis()
) {

    /** Internal list of [EntityRef]s. */
    val entities: List<EntityRef> = LinkedList()

    companion object Serializer : org.mapdb.Serializer<SchemaHeader> {

        /** The version of the Cottontail DB [DefaultSchema] file. */
        const val VERSION: Short = 2

        override fun serialize(out: DataOutput2, value: SchemaHeader) {
            out.writeShort(VERSION.toInt())
            out.writeUTF(value.name)
            out.writeLong(value.created)
            out.writeLong(value.modified)
            out.packInt(value.entities.size)
            value.entities.forEach { EntityRef.serialize(out, it) }
        }

        override fun deserialize(input: DataInput2, available: Int): SchemaHeader {
            val version = input.readShort()
            if (version != EntityHeader.VERSION) throw DatabaseException.VersionMismatchException(
                version,
                EntityHeader.VERSION
            )
            val header = SchemaHeader(input.readUTF(), input.readLong(), input.readLong())
            repeat(input.unpackInt()) {
                header.addEntityRef(
                    EntityRef.deserialize(
                        input,
                        available
                    )
                )
            }
            return header
        }
    }

    /**
     * Adds an [EntityRef] to this [SchemaHeader].
     *
     * @param ref The [EntityRef] to add (must be unique).
     */
    fun addEntityRef(ref: EntityRef) {
        require(this.entities.count { it.name == ref.name } == 0) { "No entity reference with $name already exists." }
        (this.entities as LinkedList).add(ref)
    }

    /**
     * Remove [EntityRef] from this [SchemaHeader].
     *
     * @param ref The [EntityRef] to remove (must be unique).
     */
    fun removeEntityRef(name: String) {
        val ref = this.entities.find { it.name == name }
            ?: IllegalArgumentException("No entity reference for entity $name found in header.")
        (this.entities as LinkedList).remove(ref)
    }

    /**
     * Reference pointing to an entity..
     */
    data class EntityRef(val name: String, val path: Path) {
        companion object Serializer : org.mapdb.Serializer<EntityRef> {
            override fun serialize(out: DataOutput2, value: EntityRef) {
                out.writeUTF(value.name)
                out.writeUTF(value.path.toString())
            }

            override fun deserialize(input: DataInput2, available: Int): EntityRef = EntityRef(
                input.readUTF(),
                Paths.get(input.readUTF())
            )
        }
    }
}