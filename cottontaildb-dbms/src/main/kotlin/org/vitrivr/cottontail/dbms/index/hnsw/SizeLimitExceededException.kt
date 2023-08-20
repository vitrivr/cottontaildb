package org.vitrivr.cottontail.dbms.index.hnsw

/**
 * Thrown to indicate the size of the index has been exceeded.
 */
class SizeLimitExceededException
/**
 * Constructs a SizeLimitExceededException with the specified detail message.
 *
 * @param message the detail message.
 */
    (message: String?) : RuntimeException(message) {
    companion object {
        private const val serialVersionUID = 1L
    }
}