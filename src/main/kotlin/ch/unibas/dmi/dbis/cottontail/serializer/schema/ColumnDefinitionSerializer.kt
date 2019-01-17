package ch.unibas.dmi.dbis.cottontail.serializer.schema

import ch.unibas.dmi.dbis.cottontail.database.definition.ColumnDefinition
import ch.unibas.dmi.dbis.cottontail.database.definition.ColumnType

import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.mapdb.Serializer

import java.io.IOException
import java.nio.file.Paths

class ColumnDefinitionSerializer : Serializer<ColumnDefinition<*>> {
    @Throws(IOException::class)
    override fun serialize(out: DataOutput2, value: ColumnDefinition<*>) {
        out.writeChars(value.name)
        out.writeChars(value.path.toString())
        out.writeChars(value.type.name)
    }

    @Throws(IOException::class)
    override fun deserialize(input: DataInput2, available: Int): ColumnDefinition<*>? {
        return try {
            val name = input.readUTF()
            val path =  Paths.get(input.readUTF())
            val type = ColumnType.typeForName(input.readUTF())
            ColumnDefinition(name, path, type!!)
        } catch (e: NullPointerException) {
            null
        }
    }
}
