package org.vitrivr.cottontail.data

/**
 * Data import and export format used and supported by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class Format(val suffix: String) {
    /** CBOR-based format. */
    CBOR("cbor"),

    /** JSON-based format. */
    JSON("json"),

    /** CSV-based format. */
    CSV("csv");
}