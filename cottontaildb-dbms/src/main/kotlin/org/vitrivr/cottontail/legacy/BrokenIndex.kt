package org.vitrivr.cottontail.legacy

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.index.IndexType
import java.nio.file.Path

/**
 * A placeholder of an [Index] does cannot provide any functionality because it is either broken
 * or no longer supported. Still exposes basic properties of the underlying [Index].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class BrokenIndex(override val name: Name.IndexName, override val parent: Entity, val path: Path, override val type: IndexType, ) : Index {
    override val closed: Boolean = true
    override val catalogue: Catalogue = this.parent.catalogue
    override val version: DBOVersion = DBOVersion.UNDEFINED
    override val supportsIncrementalUpdate: Boolean = false
    override val supportsPartitioning: Boolean = false
    override fun newTx(context: TransactionContext): IndexTx = throw UnsupportedOperationException("Operation not supported on legacy DBO.")
    override fun close() = throw UnsupportedOperationException("Operation not supported on legacy DBO.")
}