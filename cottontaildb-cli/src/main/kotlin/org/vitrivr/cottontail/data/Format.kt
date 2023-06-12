package org.vitrivr.cottontail.data

import kotlinx.serialization.SerialFormat
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.csv.Csv
import kotlinx.serialization.json.Json

/**
 * Data import and export format used and supported by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
enum class Format(val suffix: String, val format: SerialFormat) {
    /** CBOR-based format. */
    CBOR("cbor", Cbor {  ignoreUnknownKeys = true }),

    /** JSON-based format. */
    JSON("json",  Json { allowSpecialFloatingPointValues = true; ignoreUnknownKeys = true }),

    /** CSV-based format. */
    CSV("csv", Csv {  recordSeparator = "\n"; delimiter = ','; ignoreEmptyLines = true });
}