package org.vitrivr.cottontail.dbms.index

import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.index.gg.GGIndex
import org.vitrivr.cottontail.dbms.index.hash.NonUniqueHashIndex
import org.vitrivr.cottontail.dbms.index.hash.UniqueHashIndex
import org.vitrivr.cottontail.dbms.index.lsh.superbit.SuperBitLSHIndex
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
enum class IndexType(val inexact: Boolean, val storeConfig: StoreConfig) {
    BTREE_UQ(false, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING), /* A hash based index with unique values. */

    BTREE(false, StoreConfig.WITH_DUPLICATES_WITH_PREFIXING), /* A hash based index. */

    LUCENE(false, StoreConfig.WITHOUT_DUPLICATES), /* A Lucene based index (fulltext search). */

    VAF(false, StoreConfig.WITHOUT_DUPLICATES), /* A VA file based index (for exact kNN lookup). */

    PQ(true, StoreConfig.WITHOUT_DUPLICATES), /* A product quantization based index (for approximate kNN lookup). */

    SH(true, StoreConfig.WITHOUT_DUPLICATES), /* A spectral hashing based index (for approximate kNN lookup). */

    LSH(true, StoreConfig.WITHOUT_DUPLICATES), /* A locality sensitive hashing based index for approximate kNN lookup with Lp distance. */

    LSH_SB(true, StoreConfig.WITHOUT_DUPLICATES), /* A super bit locality sensitive hashing based index for approximate kNN lookup with cosine distance. */

    GG(true, StoreConfig.WITHOUT_DUPLICATES);

    /**
     * Opens an index of this [IndexType] using the given name and [DefaultEntity].
     *
     * @param name [Name.IndexName] of the [AbstractIndex]
     * @param entity The [DefaultEntity] the desired [AbstractIndex] belongs to.
     */
    fun open(name: Name.IndexName, entity: DefaultEntity): AbstractIndex = when (this) {
        BTREE_UQ -> UniqueHashIndex(name, entity)
        BTREE -> NonUniqueHashIndex(name, entity)
        LUCENE -> LuceneIndex(name, entity)
        LSH_SB -> SuperBitLSHIndex<VectorValue<*>>(name, entity)
        VAF -> VAFIndex(name, entity)
        PQ -> PQIndex(name, entity)
        GG -> GGIndex(name, entity)
        else -> throw NotImplementedError("Index of type $this is not implemented.")
    }
}