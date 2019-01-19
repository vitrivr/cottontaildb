package ch.unibas.dmi.dbis.cottontail.model

import ch.unibas.dmi.dbis.cottontail.database.schema.ColumnType
import org.mapdb.DBException
import java.util.*

open class DatabaseException(message: String) : Throwable(message) {

    /** */
    class TransactionClosedException(tid: UUID): DBException("Transaction $tid has been closed and cannot be used anymore.")

    /** */
    class TransactionLockException(tid: UUID): DBException("Transaction $tid was unable to obtain the necessary locks.")

    /** */
    class TransactionReadOnlyException(tid: UUID): DBException("Transaction $tid is read-only and cannot be used to alter data.")

    /** */
    class ColumnNotExistException(column: String, entity: String): DBException("Column '$column' does not exist on entity '$entity'.")

    /** */
    class ColumnTypeUnexpectedException(column: String, entity: String, expected: ColumnType<*>, actual: ColumnType<*>): DBException("Column '$column' on entity '$entity' has wrong type (expected: ${expected.name}, actual: ${actual.name}).")

    /**
     *
     */
    class DataCorruptionException(message: String): DBException(message)

    /**
     *
     */
    class InvalidTupleId(tupleId: Long): DBException("The provided tuple ID $tupleId is out of bounds and therefore invalid.")
}




