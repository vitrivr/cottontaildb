package org.vitrivr.cottontail.dbms.column

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.legacy.v2.column.ColumnV2
import java.nio.file.Path

/**
 * The driver or engine of [Column], i.e., the type of data storage that is used underneath.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class ColumnEngine {
    /** A MapDB based column. */
    MAPDB,

    /** Cottontail DB's own storage format. Currently not supported and here for future support. */
    HARE;

    /**
     * Opens a [Column] of this [ColumnEngine].
     *
     * @param path The [Path] to the [Column] file.
     * @param parent The parent [DefaultEntity].
     */
    fun open(path: Path, parent: DefaultEntity): ColumnV2<Value> = when (this) {
        MAPDB -> ColumnV2(path, parent)
        else -> throw IllegalArgumentException("Column typ $this is currently not supported by Cottontail DB.")
    }

    /**
     * Creates a [Column] of this [ColumnEngine]
     *
     * @param path The location at which to create the [Column].
     * @param columnDef [ColumnDef] describing the [Column].
     * @param config The [Config] to use.
     */
    fun create(path: Path, columnDef: ColumnDef<*>, config: Config) = when (this) {
        MAPDB -> ColumnV2.initialize(path, columnDef, config.mapdb)
        else -> throw IllegalArgumentException("Column typ $this is currently not supported by Cottontail DB.")
    }
}