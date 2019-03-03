package ch.unibas.dmi.dbis.cottontail.database.index

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.index.hash.UniqueHashIndex

internal enum class IndexType {
    HASH_UQ, /* A hash based index with unique values. */
    HASH, /* A hash based index. */
    BTREE, /* A BTree based index. */
    LUCENE, /* A Lucene based index (fulltext search). */
    VAF, /* A VA file based index (for exact kNN lookup). */
    PQ, /* A product quantization  based index (for approximate kNN lookup). */
    SH; /* A spectral hashing  based index (for approximate kNN lookup). */

    /**
     * Creates or opens an index of this [IndexType] using the given name and [Entity].
     *
     * @param name Name of the [Index]
     * @param entity The [Entity] the desired [Index] belongs to.
     */
    fun open(name: String, entity: Entity): Index = when(this) {
        IndexType.HASH_UQ -> UniqueHashIndex(name, entity)
        else -> TODO()
    }
}