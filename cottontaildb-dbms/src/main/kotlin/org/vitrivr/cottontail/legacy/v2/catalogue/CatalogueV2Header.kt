package org.vitrivr.cottontail.legacy.v2.catalogue

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.general.DBOVersion
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*

/**
 * The header section of the [CatalogueV2] data structure.
 *
 * @see [CatalogueV2]
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
internal data class CatalogueV2Header(
    val uid: String = UUID.randomUUID().toString(),
    val created: Long = System.currentTimeMillis(),
    val modified: Long = System.currentTimeMillis(),
    val schemas: List<SchemaRef> = emptyList()
) {

    companion object Serializer : org.mapdb.Serializer<CatalogueV2Header> {
        override fun serialize(out: DataOutput2, value: CatalogueV2Header) {
            out.packInt(DBOVersion.V2_0.ordinal)
            out.writeUTF(value.uid)
            out.writeLong(value.created)
            out.writeLong(value.modified)
            out.packInt(value.schemas.size)
            value.schemas.forEach { SchemaRef.serialize(out, it) }
        }

        override fun deserialize(input: DataInput2, available: Int): CatalogueV2Header {
            val version = DBOVersion.values()[input.unpackInt()]
            if (version != DBOVersion.V2_0)
                throw DatabaseException.VersionMismatchException(version, DBOVersion.V2_0)
            return CatalogueV2Header(
                input.readUTF(),
                input.readLong(),
                input.readLong(),
                (0 until input.unpackInt()).map { SchemaRef.deserialize(input, available) }
            )
        }
    }

    /**
     * A reference to a schema.
     */
    data class SchemaRef(val name: String, val path: Path? = null) {
        companion object Serializer : org.mapdb.Serializer<SchemaRef> {
            override fun serialize(out: DataOutput2, value: SchemaRef) {
                out.writeUTF(value.name)
                out.writeBoolean(value.path != null)
                if (value.path != null) {
                    out.writeUTF(value.path.toString())
                }
            }

            override fun deserialize(input: DataInput2, available: Int): SchemaRef {
                return SchemaRef(
                    input.readUTF(), if (input.readBoolean()) {
                        Paths.get(input.readUTF())
                    } else {
                        null
                    }
                )
            }
        }
    }
}
