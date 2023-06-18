package org.vitrivr.cottontail.storage.serializers.tablets

/**
 * Types of [Compression] algorithms supported for [TabletSerializer] implementations.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class Compression {
    NONE, LZ4, SNAPPY;
}