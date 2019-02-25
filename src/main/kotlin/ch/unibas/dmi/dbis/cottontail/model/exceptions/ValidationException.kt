package ch.unibas.dmi.dbis.cottontail.model.exceptions


/**
 * A class of [DatabaseException]s that are thrown whenever data validation fails usually during inserts and updates.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
open class ValidationException(message: String) : DatabaseException(message) {

    /**
     * Thrown by [Index] structures whenever their update fails because of data constraints.
     *
     * @param index The FQN of the index that was affected.
     * @param message A message describing the problem.
     */
    class IndexUpdateException(index: String, message: String): ValidationException("Index update failed due to an error: $message")
}