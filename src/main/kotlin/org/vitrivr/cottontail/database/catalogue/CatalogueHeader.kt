package org.vitrivr.cottontail.database.catalogue

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

/**
 * The header section of the [Catalogue] data structure.
 *
 * @see [Catalogue]
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
internal data class CatalogueHeader(
    val uid: String = UUID.randomUUID().toString(),
    val created: Long = System.currentTimeMillis(),
    val modified: Long = System.currentTimeMillis()
) {

    /** Internal list of [SchemaRef]s. */
    val schemas: List<SchemaRef> = LinkedList()

    companion object Serializer : org.mapdb.Serializer<CatalogueHeader> {
        /** The version of the Cottontail DB [Catalogue]  file. */
        internal const val VERSION: Short = 2

        override fun serialize(out: DataOutput2, value: CatalogueHeader) {
            out.writeShort(VERSION.toInt())
            out.writeUTF(value.uid)
            out.writeLong(value.created)
            out.writeLong(value.modified)
            out.packInt(value.schemas.size)
            value.schemas.forEach { SchemaRef.serialize(out, it) }
        }

        override fun deserialize(input: DataInput2, available: Int): CatalogueHeader {
            val version = input.readShort()
            if (version != VERSION) throw DatabaseException.VersionMismatchException(
                version,
                VERSION
            )
            val header = CatalogueHeader(input.readUTF(), input.readLong(), input.readLong())
            repeat(input.unpackInt()) {
                header.addSchemaRef(SchemaRef.deserialize(input, available))
            }
            return header
        }
    }

    /**
     * Adds an [SchemaRef] to this [CatalogueHeader].
     *
     * @param ref The [SchemaRef] to add (must be unique).
     */
    fun addSchemaRef(ref: SchemaRef) {
        require(this.schemas.count { it.name == ref.name } == 0) { "Schema reference with ${ref.name} already exists." }
        (this.schemas as LinkedList).add(ref)
    }

    /**
     * Remove [SchemaRef] from this [CatalogueHeader].
     *
     * @param name The name of the [SchemaRef] to remove (must be unique).
     */
    fun removeSchemaRef(name: String) {
        val ref = this.schemas.find { it.name == name }
            ?: IllegalArgumentException("No schema reference for entity $name found in header.")
        (this.schemas as LinkedList).remove(ref)
    }

    /**
     * A reference to a schema.
     */
    data class SchemaRef(val name: String, val path: Path) {
        companion object Serializer : org.mapdb.Serializer<SchemaRef> {
            override fun serialize(out: DataOutput2, value: SchemaRef) {
                out.writeUTF(value.name)
                out.writeUTF(value.path.toString())
            }

            override fun deserialize(input: DataInput2, available: Int): SchemaRef {
                return SchemaRef(input.readUTF(), Paths.get(input.readUTF()))
            }
        }
    }
}
