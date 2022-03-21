package org.vitrivr.cottontail.dbms.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import java.io.ByteArrayInputStream


/**
 * [ComparableBinding] for [Name]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class NameBinding: ComparableBinding() {
    /** [NameBinding] for [Name.SchemaName]s. */
    object Schema: NameBinding() {
        override fun readObject(stream: ByteArrayInputStream) = Name.SchemaName(StringBinding.BINDING.readObject(stream))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            check(`object` is Name.SchemaName) { "Cannot serialize $`object` as schema name." }
            StringBinding.BINDING.writeObject(output, `object`.schemaName)
        }
    }

    /** [NameBinding] for [Name.EntityName]s. */
    object Entity: NameBinding() {
        override fun readObject(stream: ByteArrayInputStream) = Name.EntityName(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            check(`object` is Name.EntityName) { "Cannot serialize $`object` as entity name." }
            StringBinding.BINDING.writeObject(output, `object`.schemaName)
            StringBinding.BINDING.writeObject(output, `object`.entityName)
        }
    }

    /** [NameBinding] for [Name.SequenceName]s. */
    object Sequence: NameBinding() {
        override fun readObject(stream: ByteArrayInputStream) = Name.SequenceName(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            check(`object` is Name.SequenceName) { "Cannot serialize $`object` as sequence name." }
            StringBinding.BINDING.writeObject(output, `object`.schemaName)
            StringBinding.BINDING.writeObject(output, `object`.entityName)
            StringBinding.BINDING.writeObject(output, `object`.sequenceName)
        }
    }

    /** [NameBinding] for [Name.IndexName]s. */
    object Index: NameBinding() {
        override fun readObject(stream: ByteArrayInputStream) = Name.IndexName(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            check(`object` is Name.IndexName) { "Cannot serialize $`object` as index name." }
            StringBinding.BINDING.writeObject(output, `object`.schemaName)
            StringBinding.BINDING.writeObject(output, `object`.entityName)
            StringBinding.BINDING.writeObject(output, `object`.indexName)
        }
    }

    /** [NameBinding] for [Name.ColumnName]s. */
    object Column: NameBinding() {
        override fun readObject(stream: ByteArrayInputStream) = Name.ColumnName(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            check(`object` is Name.ColumnName) { "Cannot serialize $`object` as column name." }
            StringBinding.BINDING.writeObject(output, `object`.schemaName)
            StringBinding.BINDING.writeObject(output, `object`.entityName)
            StringBinding.BINDING.writeObject(output, `object`.columnName)
        }
    }
}