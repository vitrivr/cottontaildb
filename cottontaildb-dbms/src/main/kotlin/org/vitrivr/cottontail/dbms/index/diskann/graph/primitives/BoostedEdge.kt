package org.vitrivr.cottontail.dbms.index.diskann.graph.primitives

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
data class BoostedEdge<I: Comparable<I>>(val label: I, val distance: Float, val rng: Boolean)