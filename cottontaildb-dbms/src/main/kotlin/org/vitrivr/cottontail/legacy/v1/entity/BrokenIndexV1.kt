package org.vitrivr.cottontail.legacy.v1.entity

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import java.io.Closeable
import java.nio.file.Path

/**
 * A placeholder of an [Index] does cannot provide any functionality because it is either broken
 * or no longer supported. Still exposes basic properties of the underlying [Index].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class BrokenIndexV1(override val name: Name.IndexName, override val parent: Entity, val path: Path, override val type: IndexType) : Index, Closeable {
    override val catalogue: Catalogue = this.parent.catalogue
    override val version: DBOVersion = DBOVersion.UNDEFINED
    override val supportsIncrementalUpdate: Boolean = false
    override val supportsAsyncRebuild: Boolean = false
    override val supportsPartitioning: Boolean = false
    override fun newTx(context: TransactionContext): IndexTx = throw UnsupportedOperationException("Operation not supported on legacy DBO.")
    override fun newRebuilder(context: TransactionContext): AbstractIndexRebuilder<*> = throw UnsupportedOperationException("Operation not supported on legacy DBO.")
    override fun newAsyncRebuilder(): AsyncIndexRebuilder<*> = throw UnsupportedOperationException("Operation not supported on legacy DBO.")
    override fun close() = throw UnsupportedOperationException("Operation not supported on legacy DBO.")
}