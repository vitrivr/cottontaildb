package ch.unibas.dmi.dbis.cottontail.utilities.name

object NameUtilities {
    /**
     * Finds the longest, common prefix the [Name]s in the given collection share.
     *
     * @param strings Collection of [Name]s
     * @return Longest, common prefix.
     */
    public fun findLongestPrefix(names: Collection<Name>) : Name {
        val prefix = Array<String?>(3, { null })
        for (i in 0 until 3) {
            val d = names.map { it.split('.')[i] }.distinct()
            if (d.size == 1) {
                prefix[i] = d.first()
            } else {
                break
            }
        }
        return prefix.filterNotNull().joinToString (separator = ".", postfix = ".")
    }
}