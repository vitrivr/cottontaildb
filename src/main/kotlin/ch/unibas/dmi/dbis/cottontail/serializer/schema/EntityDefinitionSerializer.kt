package ch.unibas.dmi.dbis.cottontail.serializer.schema

import ch.unibas.dmi.dbis.cottontail.database.definition.EntityDefinition
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

import java.io.IOException
import java.nio.file.Paths

class EntityDefinitionSerializer : Serializer<EntityDefinition> {
    @Throws(IOException::class)
    override fun serialize(out: DataOutput2, value: EntityDefinition) {
        out.writeChars(value.name)
        out.writeChars(value.path.toString())
    }

    @Throws(IOException::class)
    override fun deserialize(input: DataInput2, available: Int): EntityDefinition {
        val name = input.readUTF()
        val path = input.readUTF()
        return EntityDefinition(name, Paths.get(path))
    }
}

