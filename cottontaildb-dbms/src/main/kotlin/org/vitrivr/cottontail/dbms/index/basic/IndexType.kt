package org.vitrivr.cottontail.dbms.index.basic

import org.vitrivr.cottontail.dbms.index.hash.BTreeIndex
import org.vitrivr.cottontail.dbms.index.hash.UQBTreeIndex
import org.vitrivr.cottontail.dbms.index.lsh.LSHIndex
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.index.pq.IVFPQIndex
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.va.VAFIndex

/**
 * A final list of types of [AbstractIndex] implementation.
 *
 * TODO: This could actually be more of a 'registry' type of facility, which would allow for extensions
 *
 * @author Ralph Gasser
 * @version 3.1.0
 */
enum class IndexType(val descriptor: IndexDescriptor<*>, val defaultEmptyState: IndexState) {
    BTREE_UQ(UQBTreeIndex, IndexState.CLEAN), /* A btree-based index with unique values. */

    BTREE(BTreeIndex, IndexState.CLEAN), /* A hash based index. */

    LUCENE(LuceneIndex, IndexState.CLEAN), /* An Apache Lucene based index (fulltext search). */

    VAF(VAFIndex, IndexState.STALE), /* A vector approximation file (VAF) based index (for exact nearest neighbour search). */

    PQ(PQIndex, IndexState.STALE), /* A product quantization (PQ) based index for approximate nearest neighbour search. */

    IVFPQ(IVFPQIndex, IndexState.STALE), /* A product quantization (PQ) based index for billion scale approximate nearest neighbour search using an inverted file. */

    LSH(LSHIndex, IndexState.STALE) /* A locality sensitive hashing (LSH) based index for approximate nearest neighbour search. */
}