package ch.unibas.dmi.dbis.cottontail.model.basics

/**
 * An objects that holds [Record] values and allows for scanning operation on those [Record] values.
 *
 * @see Record
 *
 * @author Ralph Gasser
 * @version 1.1
 */
interface Scanable {
    /**
     * Applies the provided action to each [Record] in this [Scanable].
     *
     * @param action The action that should be applied.
     */
    fun forEach(action: (Record) -> Unit)

    /**
     * Applies the provided action to each [Record] in this [Scanable] whose tuple ID lies in the provided range.
     *
     * @param from The tuple ID of the first [Record] to iterate over.
     * @param to The tuple ID of the last [Record] to iterate over.
     * @param action The action that should be applied.
     */
    fun forEach(from: Long, to: Long, action: (Record) -> Unit)

    /**
     * Applies the provided mapping function to each [Record] in this [Scanable].
     *
     * @param action The mapping function that should be applied.
     * @return Collection of the results of the mapping function.
     */
    fun <R> map(action: (Record) -> R): Collection<R>

    /**
     * Applies the provided mapping function to each [Record] in this [Scanable] whose tuple ID lies in the provided range.
     *
     * @param from The tuple ID of the first [Record] to iterate over.
     * @param to The tuple ID of the last [Record] to iterate over.
     * @param action The mapping function that should be applied.
     * @return Collection of the results of the mapping function.
     */
    fun <R> map(from: Long, to: Long, action: (Record) -> R): Collection<R>
}