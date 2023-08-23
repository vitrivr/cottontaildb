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
sealed class NameBinding {
    /** [NameBinding] for [Name.SchemaName]s. */
    object Schema: NameBinding() {
        fun fromEntry(entry: ByteIterable): Name.SchemaName = Name.SchemaName(StringBinding.entryToString(entry))
        fun toEntry(entry: Name.SchemaName): ByteIterable = StringBinding.stringToEntry(entry.schemaName)
    }

    /** [NameBinding] for [Name.EntityName]s. */
    object Entity: NameBinding() {
        fun fromEntry(entry: ByteIterable): Name.EntityName {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            return Name.EntityName(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        }
        fun toEntry(entry: Name.EntityName): ByteIterable {
            val output = LightOutputStream()
            StringBinding.BINDING.writeObject(output, entry.schemaName)
            StringBinding.BINDING.writeObject(output, entry.entityName)
            return output.asArrayByteIterable()
        }
    }

    /** [NameBinding] for [Name.SequenceName]s. */
    object Sequence: NameBinding() {
        fun fromEntry(entry: ByteIterable): Name.SequenceName {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            return Name.SequenceName(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        }
        fun toEntry(entry: Name.SequenceName): ByteIterable {
            val output = LightOutputStream()
            StringBinding.BINDING.writeObject(output, entry.schemaName)
            StringBinding.BINDING.writeObject(output, entry.sequenceName)
            return output.asArrayByteIterable()
        }
    }

    /** [NameBinding] for [Name.IndexName]s. */
    object Index: NameBinding() {
        fun fromEntry(entry: ByteIterable): Name.IndexName {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            return Name.IndexName(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        }
        fun toEntry(entry: Name.IndexName): ByteIterable {
            val output = LightOutputStream()
            StringBinding.BINDING.writeObject(output, entry.schemaName)
            StringBinding.BINDING.writeObject(output, entry.entityName)
            StringBinding.BINDING.writeObject(output, entry.indexName)
            return output.asArrayByteIterable()
        }
    }

    /** [NameBinding] for [Name.ColumnName]s. */
    object Column: NameBinding() {
        fun fromEntry(entry: ByteIterable): Name.ColumnName {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            return Name.ColumnName(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        }
        fun toEntry(entry: Name.ColumnName): ByteIterable {
            val output = LightOutputStream()
            StringBinding.BINDING.writeObject(output, entry.schemaName)
            StringBinding.BINDING.writeObject(output, entry.entityName)
            StringBinding.BINDING.writeObject(output, entry.columnName)
            return output.asArrayByteIterable()
        }
    }
}