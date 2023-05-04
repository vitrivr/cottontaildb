package org.vitrivr.cottontail.dbms.index.lucene

import org.junit.jupiter.api.Test
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.generators.StringValueGenerator
import org.vitrivr.cottontail.dbms.index.AbstractIndexTest
import org.vitrivr.cottontail.dbms.index.basic.IndexType

/**
 * A set of unit tests to test basic data insert functionality.
 *
 * @author Ralph Gasser & Silvan Heller
 * @version 1.3.0
 */
class LuceneIndexTest : AbstractIndexTest() {
    override val columns: Array<ColumnDef<*>> = arrayOf(
        ColumnDef(this.entityName.column("id"), Types.Long),
        ColumnDef(this.entityName.column("feature"), Types.String)
    )

    override val indexColumn: ColumnDef<*>
        get() = this.columns[1]

    override val indexName: Name.IndexName
        get() = this.entityName.index("idx_feature_lucene")

    override val indexType: IndexType
        get() = IndexType.LUCENE

    /** */
    private var id = 0L


    @Test
    fun testLuceneIndex() {

    }

    /**
     * Generates and returns a new, random [StandaloneRecord] for inserting into the database.
     */
    override fun nextRecord(): StandaloneRecord {
        val id = LongValue(this.id++)
        val value = StringValueGenerator.random(128, this.random)
        return StandaloneRecord(this.id, columns = this.columns, values = arrayOf(id, value))
    }
}
