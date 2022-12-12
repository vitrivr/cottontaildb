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
        override fun readObject(stream: ByteArrayInputStream) = Name.SchemaName.create(StringBinding.BINDING.readObject(stream))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            check(`object` is Name.SchemaName) { "Cannot serialize $`object` as schema name." }
            StringBinding.BINDING.writeObject(output, `object`.schema)
        }
    }

    /** [NameBinding] for [Name.EntityName]s. */
    object Entity: NameBinding() {
        override fun readObject(stream: ByteArrayInputStream) = Name.EntityName.create(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            check(`object` is Name.EntityName) { "Cannot serialize $`object` as entity name." }
            StringBinding.BINDING.writeObject(output, `object`.schema)
            StringBinding.BINDING.writeObject(output, `object`.entity)
        }
    }

    /** [NameBinding] for [Name.SequenceName]s. */
    object Sequence: NameBinding() {
        override fun readObject(stream: ByteArrayInputStream) = Name.SequenceName.create(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            check(`object` is Name.SequenceName) { "Cannot serialize $`object` as sequence name." }
            StringBinding.BINDING.writeObject(output, `object`.schema)
            StringBinding.BINDING.writeObject(output, `object`.entity)
            StringBinding.BINDING.writeObject(output, `object`.sequence)
        }
    }

    /** [NameBinding] for [Name.IndexName]s. */
    object Index: NameBinding() {
        override fun readObject(stream: ByteArrayInputStream) = Name.IndexName.create(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            check(`object` is Name.IndexName) { "Cannot serialize $`object` as index name." }
            StringBinding.BINDING.writeObject(output, `object`.schema)
            StringBinding.BINDING.writeObject(output, `object`.entity)
            StringBinding.BINDING.writeObject(output, `object`.index)
        }
    }

    /** [NameBinding] for [Name.ColumnName]s. */
    object Column: NameBinding() {
        override fun readObject(stream: ByteArrayInputStream) = Name.ColumnName.create(StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream), StringBinding.BINDING.readObject(stream))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            check(`object` is Name.ColumnName) { "Cannot serialize $`object` as column name." }
            StringBinding.BINDING.writeObject(output, `object`.schema)
            StringBinding.BINDING.writeObject(output, `object`.entity)
            StringBinding.BINDING.writeObject(output, `object`.column)
        }
    }
}