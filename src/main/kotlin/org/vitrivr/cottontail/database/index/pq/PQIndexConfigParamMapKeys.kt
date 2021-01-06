package org.vitrivr.cottontail.database.index.pq

enum class PQIndexConfigParamMapKeys(val key: String) {
    NUM_SUBSPACES("num_subspaces"),
    NUM_CENTROIDS("num_centroids"),
    LEARNING_DATA_FRACTION("learning_data_fraction"),
    PRECISION("precision"),
    K_APPROX_SCAN("k_approx_scan"), // todo: this should be a query-time configuration option
    SEED("seed"),
    COMPLEX_STRATEGY("complex_strategy"), // direct or split to real & imag
}