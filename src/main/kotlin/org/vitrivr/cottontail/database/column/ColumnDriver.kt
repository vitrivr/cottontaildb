package org.vitrivr.cottontail.database.column

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.model.values.types.Value
import java.nio.file.Path

/**
 * The driver of [Column], i.e., the type of engine that is used underneath.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class ColumnDriver {
    MAPDB,

    /** A MapDB based column. */
    HARE;
    /** Cottontail DB's own storage format. Currently not supported and here for future support. */

    /**
     * Opens a [Column] of this [ColumnDriver].
     *
     * @param path The [Path] to the [Column] file.
     * @param parent The parent [Entity].
     */
    fun open(path: Path, parent: Entity): MapDBColumn<Value> = when (this) {
        MAPDB -> MapDBColumn(path, parent)
        else -> throw IllegalArgumentException("Column typ $this is currently not supported by Cottontail DB.")
    }

    /**
     * Creates a [Column] of this [ColumnDriver]
     *
     * @param path The location at which to create the [Column].
     * @param columnDef [ColumnDef] describing the [Column].
     * @param config The [Config] to use.
     */
    fun create(path: Path, columnDef: ColumnDef<*>, config: Config) = when (this) {
        MAPDB -> MapDBColumn.initialize(path, columnDef, config.mapdb)
        else -> throw IllegalArgumentException("Column typ $this is currently not supported by Cottontail DB.")
    }
}