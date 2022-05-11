package org.vitrivr.cottontail.dbms.index

import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.Environments
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionObserver
import java.io.Closeable
import java.nio.file.Files
import java.util.*

/**
 * A [IndexRebuilder] de-couples the step uf building-up and merging the changes with the actual [Index] structure.
 *
 * This can be advantageous for [Index] structures, that require a long time to rebuild. The first (long) step can be
 * executed in a read-only [TransactionContext], using non-blocking reads while the second (shorter) step is executed
 * in a separate [TransactionContext] thereafter.
 *
 * In order to be informed about changes that happen in the meanwhile, the [IndexRebuilder] implements the
 * [TransactionObserver], which it uses to be informed about changes to the data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class IndexRebuilder(val index: Index): TransactionObserver, Closeable {

    /** The current [IndexRebuilderState] of this [IndexRebuilder]. */
    @Volatile
    var state = IndexRebuilderState.INITIALIZED

    /**
     * The [Name.EntityName] the [Index] rebuilt by this [IndexRebuilder] works with.
     *
     * Used for [Event] filtering.
     */
    protected val entityName: Name.EntityName = this.index.name.entity()

    /** Path to the temporary [Environment] */
    protected val tmpPath = this.index.catalogue.config.temporaryDataFolder().resolve("${index.type.toString().lowercase()}-rebuild-${UUID.randomUUID()}")

    /** The temporary [Environment] used by this [IndexRebuilder]. */
    protected val tmpEnvironment: Environment = Environments.newInstance(tmpPath.toFile(), this.index.catalogue.config.xodus.toEnvironmentConfig())

    /** The Xodus [Transaction] object of the temporary environment. */
    protected val tmpTx: Transaction = this.tmpEnvironment.beginExclusiveTransaction()

    /** Internal [LinkedList] of [DataEvent]s that should be processed. */
    protected val events = LinkedList<DataEvent>()

    /** An [IndexRebuilder] is only interested in [DataEvent]s that concern the [Entity] */
    override fun isRelevant(event: Event): Boolean
        = event is DataEvent && event.entity == this.entityName


    init {
        this.scan()
        this.tmpTx.flush()
        this.state = IndexRebuilderState.SCANNED
    }

    /**
     * Scans the data necessary for this [IndexRebuilder]. Usually, this takes place within an existing [TransactionContext].
     */
    protected abstract fun scan()

    /**
     * Merges this [IndexRebuilder] with its [IndexTx] using the given [TransactionContext].
     *
     * @param context The [TransactionContext] to perform the MERGE in.
     */
    fun merge(context: TransactionContext) {
        require(this.state == IndexRebuilderState.SCANNED) { "Cannot perform MERGE with index rebuilder because it is in the wrong state."}
        this.internalMerge(context)
        this.state = IndexRebuilderState.MERGED
    }

    /**
     * Aborts ongoing operations for this [IndexRebuilder] by updating the flag.
     */
    fun abort() {
        require(this.state != IndexRebuilderState.CLOSED) { "Cannot perform ABORT with index rebuilder, because it was closed."}
        this.state = IndexRebuilderState.ABORTED
    }

    /**
     *
     */
    protected abstract fun internalMerge(context: TransactionContext)

    /**
     * Closes this [IndexRebuilder].
     */
    override fun close() {
        if (this.state != IndexRebuilderState.CLOSED) {
            /* Abort transaction and close environment. */
            this.tmpTx.abort()
            this.tmpEnvironment.close()

            /* Tries to cleanup the temporary environment. */
            Files.walk(this.tmpPath).sorted(Comparator.reverseOrder()).forEach {
                try {
                    Files.delete(it)
                } catch (e: Throwable) {
                    /* No op. */
                }
            }

            /* Updates the state. */
            this.state = IndexRebuilderState.CLOSED
        }
    }
}