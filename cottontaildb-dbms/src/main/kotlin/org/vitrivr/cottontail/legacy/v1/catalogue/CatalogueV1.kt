package org.vitrivr.cottontail.legacy.v1.catalogue

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.*
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.functions.FunctionRegistry
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.legacy.v1.schema.SchemaV1
import java.nio.file.Files
import java.nio.file.Path
import java.util.*


/**
 * The main catalogue in Cottontail DB. It contains references to all the [SchemaV1]s managed by Cottontail
 * and is the main way of accessing these [SchemaV1]s and creating new ones.
 *
 * @see SchemaV1
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class CatalogueV1(override val config: Config) : Catalogue {
    /**
     * Companion object to [CatalogueV1]
     */
    companion object {
        /** ID of the schema header! */
        internal const val HEADER_RECORD_ID: Long = 1L

        /** Filename for the [CatalogueV1] catalogue file.  */
        internal const val FILE_CATALOGUE = "catalogue.db"
    }

    override val functions: FunctionRegistry
        get() = throw UnsupportedOperationException("Operation not supported on legacy DBO.")

    /** Root to Cottontail DB root folder. */
    val path: Path = config.root

    /** Constant name of the [CatalogueV1] object. */
    override val name: Name.RootName = Name.RootName

    /** Constant parent [DBO], which is null in case of the [CatalogueV1]. */
    override val parent: DBO? = null

    /** The [StoreWAL] that contains the Cottontail DB catalogue. */
    private val store: CottontailStoreWAL =
        this.config.mapdb.store(this.path.resolve(FILE_CATALOGUE))

    /** Reference to the [CatalogueV1Header] of the [CatalogueV1]. Accessing it will read right from the underlying store. */
    private val header: CatalogueV1Header
        get() = this.store.get(HEADER_RECORD_ID, CatalogueV1Header.Serializer)
            ?: throw DatabaseException.DataCorruptionException("Failed to open Cottontail DB catalogue header!")

    /** A in-memory registry of all the [SchemaV1]s contained in this [CatalogueV1]. When a [CatalogueV1] is opened, all the [SchemaV1]s will be loaded. */
    private val registry: MutableMap<Name.SchemaName, SchemaV1> =
        Collections.synchronizedMap(Object2ObjectOpenHashMap())

    /** Size of this [CatalogueV1] in terms of [SchemaV1]s it contains. */
    val size: Int
        get() =  this.header.schemas.size

    /** The [Catalogue] this [CatalogueV1] belongs to. */
    override val catalogue: Catalogue
        get() = this

    /** The [DBOVersion] of this [CatalogueV1]. */
    override val version: DBOVersion
        get() = DBOVersion.V1_0

    /** Status indicating whether this [CatalogueV1] is open or closed. */
    @Volatile
    var closed: Boolean = false
        private set

    /** Initialization logic for [CatalogueV1]. */
    init {
        val header = this.header
        for (sid in header.schemas) {
            val schema = this.store.get(sid, CatalogueV1Header.CatalogueEntry.Serializer)
                ?: throw DatabaseException.DataCorruptionException("Failed to open Cottontail DB catalogue entry!")
            val path = this.path.resolve("schema_${schema.name}")
            if (!Files.exists(path)) {
                throw DatabaseException.DataCorruptionException("Broken catalogue entry for schema '${schema.name}'. Path $path does not exist!")
            }
            val s = SchemaV1(Name.SchemaName(schema.name), this)
            this.registry[s.name] = s
        }
    }

    /**
     * Creates and returns a new [CatalogueV1.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [CatalogueV1.Tx] for.
     * @return New [CatalogueV1.Tx]
     */
    override fun newTx(context: TransactionContext): CatalogueTx = Tx(context)

    /**
     * Closes the [CatalogueV1] and all objects contained within.
     */
    override fun close() {
        this.registry.forEach { (_, v) -> v.close() }
        this.store.close()
        this.closed = true
    }

    /**
     * A [Tx] that affects this [CatalogueV1].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), CatalogueTx {

        /** Reference to the [CatalogueV1] this [CatalogueTx] belongs to. */
        override val dbo: Catalogue
            get() = this@CatalogueV1

        /**
         * Returns a list of [Name.SchemaName] held by this [CatalogueV1].
         *
         * @return [List] of all [Name.SchemaName].
         */
        override fun listSchemas(): List<Name.SchemaName> {
            return this@CatalogueV1.registry.values.map { it.name }
        }

        /**
         * Returns the [SchemaV1] for the given [Name.SchemaName].
         *
         * @param name [Name.SchemaName] to obtain the [SchemaV1] for.
         */
        override fun schemaForName(name: Name.SchemaName): SchemaV1 {
            return this@CatalogueV1.registry[name] ?: throw DatabaseException.SchemaDoesNotExistException(name)
        }

        override fun createSchema(name: Name.SchemaName): Schema {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun dropSchema(name: Name.SchemaName) {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }
    }
}