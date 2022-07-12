package org.vitrivr.cottontail.legacy.v2.entity

import org.mapdb.DB
import org.mapdb.DataInput2
import org.mapdb.DataOutput2
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import java.io.Closeable
import java.nio.file.Path

/**
 * A placeholder of an [Index] does cannot provide any functionality because it is either broken
 * or no longer supported. Still exposes basic properties of the underlying [Index].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class BrokenIndexV2(override val name: Name.IndexName, override val parent: Entity, val path: Path) : Index, Closeable {
    companion object {
        /** Field name for the [IndexHeader] entry.  */
        const val INDEX_HEADER_FIELD = "cdb_index_header"
    }
    /** The internal [DB] reference for this [BrokenIndexV2]. */
    private val store: DB = this.parent.parent.parent.config.mapdb.db(this.path)

    /** The [IndexHeader] for this [BrokenIndexV2]. */
    private val headerField = this.store.atomicVar(INDEX_HEADER_FIELD, IndexHeader.Serializer).createOrOpen()

    /** The [ColumnDef] that are covered (i.e. indexed) by this [BrokenIndexV2]. */
    val columns: Array<ColumnDef<*>> = this.headerField.get().columns
    override val catalogue: Catalogue = this.parent.catalogue
    override val version: DBOVersion = DBOVersion.UNDEFINED
    override val supportsIncrementalUpdate: Boolean = false
    override val supportsAsyncRebuild: Boolean = false
    override val supportsPartitioning: Boolean = false
    override val type: IndexType
        get() = this.headerField.get().type

    override fun newTx(context: TransactionContext): IndexTx = throw UnsupportedOperationException("Operation not supported on legacy DBO.")
    override fun newRebuilder(context: TransactionContext): AbstractIndexRebuilder<*> = throw UnsupportedOperationException("Operation not supported on legacy DBO.")
    override fun newAsyncRebuilder(): AsyncIndexRebuilder<*> = throw UnsupportedOperationException("Operation not supported on legacy DBO.")
    override fun close() {
        this.store.close()
    }

    /**
     * The header section of an [BrokenIndexV2] data structure.
     *
     * @see BrokenIndexV2
     *
     * @author Ralph Gasser
     * @version 2.0.0
     */
    data class IndexHeader(val name: String, val type: IndexType, val columns: Array<ColumnDef<*>>, val created: Long = System.currentTimeMillis(), val modified: Long = System.currentTimeMillis()) {
        companion object Serializer : org.mapdb.Serializer<IndexHeader> {
            override fun serialize(out: DataOutput2, value: IndexHeader) {
                out.packInt(DBOVersion.V2_0.ordinal)
                out.writeUTF(value.name)
                out.packInt(value.type.ordinal)
                out.packInt(value.columns.size)
                value.columns.forEach {
                    out.writeUTF(it.name.toString())
                    out.packInt(it.type.ordinal)
                    out.packInt(it.type.logicalSize)
                    out.writeBoolean(it.nullable)
                    out.writeBoolean(it.primary)
                }
            }

            override fun deserialize(input: DataInput2, available: Int): IndexHeader {
                val version = DBOVersion.values()[input.unpackInt()]
                if (version != DBOVersion.V2_0)
                    throw DatabaseException.VersionMismatchException(version, DBOVersion.V2_0)
                return IndexHeader(
                    input.readUTF(),
                    IndexTypeV2.values()[input.unpackInt()].toIndexType(),
                    Array(input.unpackInt()) {
                        val components = input.readUTF().split('.').toTypedArray()
                        val name = Name.ColumnName(components[1], components[2], components[3])
                        ColumnDef(name, Types.forOrdinal(input.unpackInt(), input.unpackInt()), input.readBoolean(), input.readBoolean())
                    }
                )
            }
        }
        /**
         * The enum list of [IndexType]s.
         *
         * @author Ralph Gasser
         * @version 2.0.0
         */
        private enum class IndexTypeV2 {
            HASH_UQ,
            HASH,
            BTREE,
            LUCENE,
            VAF,
            PQ,
            SH,
            LSH,
            LSH_SB,
            GG;

            /**
             * Converts this [IndexTypeV2] to [IndexType].
             *
             * @return [IndexType]
             */
            fun toIndexType(): IndexType = when(this) {
                HASH_UQ -> IndexType.BTREE_UQ
                HASH,
                BTREE -> IndexType.BTREE
                LUCENE -> IndexType.LUCENE
                VAF -> IndexType.VAF
                PQ -> IndexType.PQ
                LSH,
                LSH_SB -> IndexType.LSH
                else -> throw UnsupportedOperationException("The index type ${this} is no longer supported by Cottontail DB. ")
            }
        }
    }
}