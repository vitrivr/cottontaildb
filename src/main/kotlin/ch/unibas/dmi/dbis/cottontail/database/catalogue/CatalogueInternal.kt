package ch.unibas.dmi.dbis.cottontail.database.schema.catalogue

import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer
import java.nio.file.Path

/**
 * The header section of the [Catalogue] data structure.
 *
 * @see [Catalogue]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class CatalogueHeader(var size: Long = 0, var created: Long = System.currentTimeMillis(), var modified: Long  = System.currentTimeMillis()) {
    companion object {
        /** The identifier that is used to identify a Cottontail DB [Catalogue] file. */
        internal const val IDENTIFIER: String = "COTTONT_CAT"

        /** The version of the Cottontail DB [Catalogue]  file. */
        internal const val VERSION: Short = 1
    }
}

/**
 * The [Serializer] for the [CatalogueHeader].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal object CatalogueHeaderSerializer : Serializer<CatalogueHeader> {
    override fun serialize(out: DataOutput2, value: CatalogueHeader) {
        out.writeUTF(CatalogueHeader.IDENTIFIER)
        out.writeShort(CatalogueHeader.VERSION.toInt())
        out.packLong(value.size)
        out.writeLong(value.created)
        out.writeLong(value.modified)
    }

    override fun deserialize(input: DataInput2, available: Int): CatalogueHeader {
        if (!validate(input)) {
            throw DatabaseException.InvalidFileException("Cottontail DB Entity")
        }
        val size = input.unpackLong()
        val created = input.readLong()
        val modified = input.readLong()
        return CatalogueHeader(size, created, modified)
    }

    /**
     * Validates the [CatalogueHeader]. Must be executed before deserialization
     *
     * @return True if validation was successful, false otherwise.
     */
    private fun validate(input: DataInput2): Boolean {
        val identifier = input.readUTF()
        val version = input.readShort()
        return (version == CatalogueHeader.VERSION) and (identifier == CatalogueHeader.IDENTIFIER)
    }
}

/**
 * An entry in the [Catalogue] corresponding to a [Schema].
 *
 * @see [Catalogue]
 * @see [Schema]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal data class CatalogueEntry(val name: String, val path: Path)


