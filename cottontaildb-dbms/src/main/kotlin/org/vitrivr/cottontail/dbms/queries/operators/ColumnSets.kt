package org.vitrivr.cottontail.dbms.queries.operators

import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types


/**
 * A collection of columns returned by common operators (mostly DDL & DML operations).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ColumnSets {
    /** Column returned by most DDL statements. */
    val DDL_STATUS_COLUMNS: List<ColumnDef<*>> = listOf(
        ColumnDef(Name.ColumnName("action"), Types.String, false),
        ColumnDef(Name.ColumnName("dbo"), Types.String, false),
        ColumnDef(Name.ColumnName("duration_ms"), Types.Long, false)
    )

    /** The columns returned by LIST ENTITY and LIST SCHEMA operations. */
    val DDL_LIST_COLUMNS: List<ColumnDef<*>> = listOf(
        ColumnDef(Name.ColumnName(Constants.COLUMN_NAME_DBO), Types.String, false),
        ColumnDef(Name.ColumnName(Constants.COLUMN_NAME_CLASS), Types.String, false)
    )

    /** The columns returned by ABOUT ENTITY operation. */
    val DDL_ABOUT_COLUMNS: List<ColumnDef<*>> = listOf(
        ColumnDef(Name.ColumnName("dbo"), Types.String, false),
        ColumnDef(Name.ColumnName("class"), Types.String, false),
        ColumnDef(Name.ColumnName("type"), Types.String, true),
        ColumnDef(Name.ColumnName("rows"), Types.Int, true),
        ColumnDef(Name.ColumnName("l_size"), Types.Int, true),
        ColumnDef(Name.ColumnName("nullable"), Types.Boolean, true),
        ColumnDef(Name.ColumnName("info"), Types.String, true)
    )

    /** The columns returned by ABOUT ENTITY operation. */
    val DDL_INTROSPECTION_COLUMNS: List<ColumnDef<*>> = listOf(
        ColumnDef(Name.ColumnName("dbo"), Types.String, false),
        ColumnDef(Name.ColumnName("key"), Types.String, false),
        ColumnDef(Name.ColumnName("value"), Types.String, false),
    )

    /** The columns returned by LIST LOCKS operation. */
    val DDL_LOCKS_COLUMNS: List<ColumnDef<*>> = listOf(
        ColumnDef(Name.ColumnName("dbo"), Types.String, false),
        ColumnDef(Name.ColumnName("mode"), Types.String, false),
        ColumnDef(Name.ColumnName("owner_count"), Types.Int, false),
        ColumnDef(Name.ColumnName("owners"), Types.String, false)
    )

    /** The columns returned by LIST TRANSACTIONS operation. */
    val DDL_TRANSACTIONS_COLUMNS: List<ColumnDef<*>> = listOf(
        ColumnDef(Name.ColumnName("txId"), Types.Long, false),
        ColumnDef(Name.ColumnName("type"), Types.String, false),
        ColumnDef(Name.ColumnName("state"), Types.String, false),
        ColumnDef(Name.ColumnName("lock_count"), Types.Int, false),
        ColumnDef(Name.ColumnName("tx_count"), Types.Int, false),
        ColumnDef(Name.ColumnName("created"), Types.Date, false),
        ColumnDef(Name.ColumnName("ended"), Types.Date, true),
        ColumnDef(Name.ColumnName("duration[s]"), Types.Double, true)
    )
}