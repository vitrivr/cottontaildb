package ch.unibas.dmi.dbis.cottontail.model.exceptions

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef

/**
 * These exceptions are thrown whenever a query cannot be executed properly.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
open class QueryException(message: String) : DatabaseException(message) {

    /**
     *
     */
    class ColumnDoesNotExistException(column: ColumnDef<*>): DatabaseException("The column $column does not exist!")

    /**
     *
     */
    class TypeException(): DatabaseException("The provided value is not of the expected type.")

}