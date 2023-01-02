package org.vitrivr.cottontail.legacy.v2.catalogue

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.DB
import org.mapdb.StoreWAL
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.FunctionRegistry
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.*
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.legacy.v2.schema.SchemaV2
import org.vitrivr.cottontail.utilities.extensions.read
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * The default [Catalogue] implementation based on Map DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class CatalogueV2(override val config: Config) : Catalogue {
    /**
     * Companion object to [CatalogueV2]
     */
    companion object {
        /** ID of the schema header! */
        internal const val CATALOGUE_HEADER_FIELD: String = "cdb_catalogue_header"

        /** Filename for the [DefaultEntity] catalogue.  */
        internal const val FILE_CATALOGUE = "catalogue.db"
    }

    /** Root to Cottontail DB root folder. */
    val path: Path = config.root

    /** Constant name of the [CatalogueV2] object. */
    override val name: Name.RootName
        get() = Name.RootName

    /** The [Catalogue] this [CatalogueV2] belongs to. */
    override val catalogue: Catalogue
        get() = this

    /** The [DBOVersion] of this [CatalogueV2]. */
    override val version: DBOVersion
        get() = DBOVersion.V2_0

    /** Constant parent [DBO], which is null in case of the [CatalogueV2]. */
    override val parent: DBO? = null

    /** A lock used to mediate access to this [CatalogueV2]. */
    private val closeLock = StampedLock()

    /** The [StoreWAL] that contains the Cottontail DB catalogue. */
    private val store: DB = this.config.mapdb.db(this.path.resolve(FILE_CATALOGUE))

    /** Reference to the [CatalogueV2Header] of the [CatalogueV2]. Accessing it will read right from the underlying store. */
    private val headerField = this.store.atomicVar(CATALOGUE_HEADER_FIELD, CatalogueV2Header.Serializer).createOrOpen()

    /** An in-memory registry of all the [Schema]s contained in this [CatalogueV2]. When a [Catalogue] is opened, all the [Schema]s will be loaded. */
    private val registry: MutableMap<Name.SchemaName, SchemaV2> = Collections.synchronizedMap(Object2ObjectOpenHashMap())

    /** The [FunctionRegistry] exposed by this [Catalogue]. */
    override val functions: FunctionRegistry
        get() = throw UnsupportedOperationException("Operation not supported on legacy DBO.")

    /** Size of this [CatalogueV2] in terms of [Schema]s it contains. This is a snapshot and may change anytime! */
    val size: Int
        get() = this.closeLock.read { this.headerField.get().schemas.size }

    /** Status indicating whether this [CatalogueV2] is open or closed. */
    override val closed: Boolean
        get() = this.store.isClosed()

    init {
        /* Initialize empty catalogue */
        if (this.headerField.get() == null) {
            this.headerField.set(CatalogueV2Header())
            this.store.commit()
        }

        /* Initialize schemas. */
        for (schemaRef in this.headerField.get().schemas) {
            if (schemaRef.path != null && Files.exists(schemaRef.path)) {
                this.registry[Name.SchemaName.create(schemaRef.name)] = SchemaV2(path, this)
            } else {
                val path = this.path.resolve("schema_${schemaRef.name}")
                if (!Files.exists(path)) {
                    throw DatabaseException.DataCorruptionException("Broken catalogue entry for schema '${schemaRef.name}'. Path $path does not exist!")
                }
                this.registry[Name.SchemaName.create(schemaRef.name)] = SchemaV2(path, this)
            }
        }
    }

    /**
     * Creates and returns a new [CatalogueV2.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [CatalogueV2.Tx] for.
     * @return New [CatalogueV2.Tx]
     */
    override fun newTx(context: TransactionContext): Tx = Tx(context)

    /**
     * Closes the [CatalogueV2] and all objects contained within.
     */
    override fun close() = this.closeLock.write {
        this.store.close()
        this.registry.forEach { (_, v) -> v.close() }
    }

    /**
     * A [Tx] that affects this [CatalogueV2].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), CatalogueTx {

        /** Reference to the [CatalogueV2] this [CatalogueTx] belongs to. */
        override val dbo: CatalogueV2
            get() = this@CatalogueV2

        /** Obtains a global (non-exclusive) read-lock on [CatalogueV2]. Prevents enclosing [Schema] from being closed. */
        private val closeStamp = this@CatalogueV2.closeLock.readLock()

        /** Checks if DBO is still open. */
        init {
            if (this@CatalogueV2.closed) {
                this@CatalogueV2.closeLock.unlockRead(this.closeStamp)
                throw TransactionException.DBOClosed(this.context.txId, this@CatalogueV2)
            }
        }

        /**
         * Returns a list of [Name.SchemaName] held by this [CatalogueV2].
         *
         * @return [List] of all [Name.SchemaName].
         */
        override fun listSchemas(): List<Name.SchemaName> {
            return this@CatalogueV2.registry.keys.toList()
        }

        /**
         * Returns the [Schema] for the given [Name.SchemaName].
         *
         * @param name [Name.SchemaName] to obtain the [Schema] for.
         */
        override fun schemaForName(name: Name.SchemaName): Schema {
            return this@CatalogueV2.registry[name] ?: throw DatabaseException.SchemaDoesNotExistException(name)
        }

        override fun createSchema(name: Name.SchemaName): Schema {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun dropSchema(name: Name.SchemaName) {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        /**
         * Releases the [closeLock] on the [CatalogueV2].
         */
        override fun cleanup() {
            this@CatalogueV2.closeLock.unlockRead(this.closeStamp)
        }
    }
}