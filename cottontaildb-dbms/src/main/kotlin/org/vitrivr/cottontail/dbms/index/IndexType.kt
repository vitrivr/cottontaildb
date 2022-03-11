package org.vitrivr.cottontail.dbms.index

import org.vitrivr.cottontail.dbms.index.gg.GGIndex
import org.vitrivr.cottontail.dbms.index.hash.BTreeIndex
import org.vitrivr.cottontail.dbms.index.hash.UQBTreeIndex
import org.vitrivr.cottontail.dbms.index.lsh.LSHIndex
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.va.VAFIndex

/**
 * A final list of types of [AbstractIndex] implementation.
 *
 * TODO: This could actually be more of a 'registry' type of facility, which would allow for extensions
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
enum class IndexType(val descriptor: IndexDescriptor<*>) {
    BTREE_UQ(UQBTreeIndex), /* A btree-based index with unique values. */

    BTREE(BTreeIndex), /* A hash based index. */

    LUCENE(LuceneIndex), /* An Apache Lucene based index (fulltext search). */

    VAF(VAFIndex), /* A vector approximation file (VAF) based index (for exact nearest neighbour search). */

    PQ(PQIndex), /* A product quantization (PQ) based index (for approximate nearest neighbour search). */

    LSH(LSHIndex), /* A locality sensitive hashing (LSH) based index (for approximate nearest neighbour search). */

    GG(GGIndex);  /* A greedy grouping (GG) based index (for nearest neighbour search). */
}