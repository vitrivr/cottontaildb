package org.vitrivr.cottontail.database.index.lsh.superbit

/**
 * Enum with keys used by the [SuperBitLSHIndexConfig] parameter maps.
 *
 * @author Gabriel Zihlmann
 * @version 1.0.0
 */
enum class SuperBitSLHIndexConfigParamMapKeys(val key: String) {
    SUPERBIT_DEPTH("superbitdepth"),
    SUPERBITS_PER_STAGE("superbitsperstage"),
    SEED("seed"),
    NUM_STAGES("stages"),
    NUM_BUCKETS("buckets"),
    CONSIDER_IMAGINARY("considerimaginary"),
    SAMPLING_METHOD("samplingmethod")
}