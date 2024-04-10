package org.vitrivr.cottontail.dbms.queries.operators

import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_ACTION
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_CLASS
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_DBO
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_DURATION
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_NULLABLE
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_ROWS
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_SIZE
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_TYPE
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types


/**
 * A collection of columns returned by common operators (mostly DDL & DML operations).
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object ColumnSets {
    /** Column returned by most DDL statements. */
    val DDL_STATUS_COLUMNS: List<ColumnDef<*>> = listOf(
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_ACTION), Types.String, false),
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_DBO), Types.String, false),
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_DURATION), Types.Long, false)
    )

    /** The columns returned by LIST ENTITY and LIST SCHEMA operations. */
    val DDL_LIST_COLUMNS: List<ColumnDef<*>> = listOf(
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_DBO), Types.String, false),
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_CLASS), Types.String, false)
    )

    /** The columns returned by ABOUT ENTITY operation. */
    val DDL_ABOUT_COLUMNS: List<ColumnDef<*>> = listOf(
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_DBO), Types.String, false),
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_CLASS), Types.String, false),
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_TYPE), Types.String, true),
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_ROWS), Types.Long, true),
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_SIZE), Types.Int, true),
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_NULLABLE), Types.Boolean, true),
        ColumnDef(Name.ColumnName.create("info"), Types.String, true)
    )

    /** The columns returned by ABOUT ENTITY operation. */
    val DDL_INTROSPECTION_COLUMNS: List<ColumnDef<*>> = listOf(
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_DBO), Types.String, false),
        ColumnDef(Name.ColumnName.create("key"), Types.String, false),
        ColumnDef(Name.ColumnName.create("value"), Types.String, false),
    )

    /** The columns returned by LIST LOCKS operation. */
    val DDL_LOCKS_COLUMNS: List<ColumnDef<*>> = listOf(
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_DBO), Types.String, false),
        ColumnDef(Name.ColumnName.create("mode"), Types.String, false),
        ColumnDef(Name.ColumnName.create("owner_count"), Types.Int, false),
        ColumnDef(Name.ColumnName.create("owners"), Types.String, false)
    )

    /** The columns returned by LIST TRANSACTIONS operation. */
    val DDL_TRANSACTIONS_COLUMNS: List<ColumnDef<*>> = listOf(
        ColumnDef(Name.ColumnName.create("txId"), Types.Long, false),
        ColumnDef(Name.ColumnName.create(COLUMN_NAME_TYPE), Types.String, false),
        ColumnDef(Name.ColumnName.create("state"), Types.String, false),
        ColumnDef(Name.ColumnName.create("created"), Types.Date, false),
        ColumnDef(Name.ColumnName.create("ended"), Types.Date, true),
        ColumnDef(Name.ColumnName.create("duration[s]"), Types.Double, true)
    )
}