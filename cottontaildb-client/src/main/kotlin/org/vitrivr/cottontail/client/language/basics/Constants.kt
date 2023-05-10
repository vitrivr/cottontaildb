package org.vitrivr.cottontail.client.language.basics

/**
 * Constants used in Cottontail DB client.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Constants {
    /** Constant used in DDL responses. Fully qualified name of the database object (DBO). */
    const val COLUMN_NAME_DBO = "dbo"

    /** Constant used in DDL responses. [Class] of the database object (DBO). Possible values: SCHEMA, ENTITY, COLUMN, INDEX */
    const val COLUMN_NAME_CLASS = "class"

    /** Constant used in DDL responses. Type of a COLUMN database object (DBO). */
    const val COLUMN_NAME_TYPE = "type"

    /** Constant used in DDL responses. Logical size of a COLUMN database object (DBO). */
    const val COLUMN_NAME_SIZE = "l_size"

    /** Constant used in DDL responses. Whether a COLUMN database object (DBO) is nullable or not. */
    const val COLUMN_NAME_NULLABLE = "nullable"

    /** Constant used in DDL responses. Number of rows in a ENTITY database object (DBO). */
    const val COLUMN_NAME_ROWS = "rows"

    /** The maximum message size in bytes (slightly smaller than 4MB). */
    const val MAX_PAGE_SIZE_BYTES = 3_750_000
}