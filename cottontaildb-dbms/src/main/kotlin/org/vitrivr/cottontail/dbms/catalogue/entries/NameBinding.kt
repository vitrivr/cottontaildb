package org.vitrivr.cottontail.dbms.catalogue.entries

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name

/**
 * [ComparableBinding] for [Name]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface NameBinding {

    /** [NameBinding] for [Name.SchemaName]s. */
    data object Schema: NameBinding {
        fun fromEntry(entry: ByteIterable): Name.SchemaName = Name.SchemaName.create(StringBinding.entryToString(entry))
        fun toEntry(entry: Name.SchemaName): ByteIterable = StringBinding.stringToEntry(entry.schema.lowercase())
    }

    /** [NameBinding] for [Name.EntityName]s. */
    data object Entity: NameBinding {
        fun fromEntry(entry: ByteIterable): Name.EntityName {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            return Name.EntityName.create(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        }
        fun toEntry(entry: Name.EntityName): ByteIterable {
            val output = LightOutputStream()
            StringBinding.BINDING.writeObject(output, entry.schema.lowercase())
            StringBinding.BINDING.writeObject(output, entry.entity.lowercase())
            return output.asArrayByteIterable()
        }
    }

    /** [NameBinding] for [Name.SequenceName]s. */
    data object Sequence: NameBinding {
        fun fromEntry(entry: ByteIterable): Name.SequenceName {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            return Name.SequenceName.create(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        }
        fun toEntry(entry: Name.SequenceName): ByteIterable {
            val output = LightOutputStream()
            StringBinding.BINDING.writeObject(output, entry.schema.lowercase())
            StringBinding.BINDING.writeObject(output, entry.sequence.lowercase())
            return output.asArrayByteIterable()
        }
    }

    /** [NameBinding] for [Name.IndexName]s. */
    data object Index: NameBinding {
        fun fromEntry(entry: ByteIterable): Name.IndexName {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            return Name.IndexName.create(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        }
        fun toEntry(entry: Name.IndexName): ByteIterable {
            val output = LightOutputStream()
            StringBinding.BINDING.writeObject(output, entry.schema.lowercase())
            StringBinding.BINDING.writeObject(output, entry.entity.lowercase())
            StringBinding.BINDING.writeObject(output, entry.index.lowercase())
            return output.asArrayByteIterable()
        }
    }

    /** [NameBinding] for [Name.ColumnName]s. */
    data object Column: NameBinding {
        fun fromEntry(entry: ByteIterable): Name.ColumnName {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            return Name.ColumnName.create(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        }
        fun toEntry(entry: Name.ColumnName): ByteIterable {
            val output = LightOutputStream()
            StringBinding.BINDING.writeObject(output, entry.schema.lowercase())
            StringBinding.BINDING.writeObject(output, entry.entity.lowercase())
            StringBinding.BINDING.writeObject(output, entry.column.lowercase())
            return output.asArrayByteIterable()
        }
    }
}