package org.vitrivr.cottontail.dbms.index.hnsw

import java.io.Serializable
import java.util.*

/**
 * Result of a nearest neighbour search.
 *
 * @param <TItem> type of the item returned
 * @param <TDistance> type of the distance returned by the configured distance function
</TDistance></TItem> */
class SearchResult<TItem, TDistance>
/**
 * Constructs a new SearchResult instance.
 *
 * @param item the item
 * @param distance the distance from the search query
 * @param distanceComparator used to compare distances
 */(private val item: TItem, private val distance: TDistance, private val distanceComparator: Comparator<TDistance>) :
    Comparable<SearchResult<TItem, TDistance>>, Serializable {
    /**
     * Returns the item.
     *
     * @return the item
     */
    fun item(): TItem {
        return item
    }

    /**
     * Returns the distance from the search query.
     *
     * @return the distance from the search query
     */
    fun distance(): TDistance {
        return distance
    }

    /**
     * {@inheritDoc}
     */
    override fun compareTo(other: SearchResult<TItem, TDistance>): Int {
        return distanceComparator.compare(distance, other.distance)
    }

    /**
     * {@inheritDoc}
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val that = other as SearchResult<*, *>
        return distance == that.distance && item == that.item
    }

    /**
     * {@inheritDoc}
     */
    override fun hashCode(): Int {
        return Objects.hash(distance, item)
    }

    /**
     * {@inheritDoc}
     */
    override fun toString(): String {
        return "SearchResult{" +
                "distance=" + distance +
                ", item=" + item +
                '}'
    }

    companion object {
        private const val serialVersionUID = 1L

        /**
         * Convenience method for creating search results who's distances are Comparable.
         *
         * @param item the item
         * @param distance the distance from the search query
         * @param <TItem> type of the item returned
         * @param <TDistance> type of the distance returned by the configured distance function
         * @return new SearchResult instance
        </TDistance></TItem> */
        @JvmStatic
        fun <TItem, TDistance : Comparable<TDistance>?> create(
            item: TItem,
            distance: TDistance
        ): SearchResult<TItem, TDistance> {
            return SearchResult(item, distance, Comparator.naturalOrder())
        }
    }
}