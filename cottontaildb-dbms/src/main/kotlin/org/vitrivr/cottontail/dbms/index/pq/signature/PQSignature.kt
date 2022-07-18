package org.vitrivr.cottontail.dbms.index.pq.signature

/**
 * A [PQSignature] as used by any PQ based index. Wraps a [ShortArray].
 *
 * @author  Ralph Gasser
 * @version 1.0.0
 */
interface PQSignature {
    val cells: ShortArray
}