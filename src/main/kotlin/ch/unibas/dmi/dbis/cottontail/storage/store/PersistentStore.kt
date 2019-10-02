package ch.unibas.dmi.dbis.cottontail.storage.store

/**
 * An abstract representation over a facility that can hold data (a data [Store]) and allows for random access and stores
 * the data it holds in a persistent fashion (i.e. backed by an I/O device).
 *
 * @version 1.0
 * @author Ralph Gasser
 */
interface PersistentStore : Store {

    /**
     * Forces all changes made to this [PersistentStore] to the underlying I/O device. Some [PersistentStore]s may not
     * support this, in which case they will always return false.
     *
     * @return State of the force action.
     */
    fun force(): Boolean

    /**
     * Attempts to load all the data held by this [PersistentStore] into physical.
     */
    fun load()

    /** Always returns true, since [PersistentStore] are persistent by definition. */
    override val isPersistent: Boolean
        get() = true
}