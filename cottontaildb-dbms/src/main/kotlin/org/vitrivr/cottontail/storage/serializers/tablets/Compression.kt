package org.vitrivr.cottontail.storage.serializers.tablets

/**
 * Types of [Compression] algorithms supported for [TabletSerializer] implementations.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class Compression(val direct: Boolean) {
    NONE(false),
    LZ4(false),
    SNAPPY(true);
}