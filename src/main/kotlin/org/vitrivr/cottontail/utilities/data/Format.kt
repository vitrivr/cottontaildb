package org.vitrivr.cottontail.utilities.data

/**
 * Data import and export format used and supported by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
enum class Format {
    PROTO,

    /** ProtocolBuffer format. */
    JSON,

    /** JSON format. */
    CSV,
    /** CSV format. */
}