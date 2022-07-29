package org.vitrivr.cottontail.utilities.selection

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import java.lang.Float.max
import java.lang.Float.min
import java.util.*

/**
 * Simple test case for [HeapSelection] class, which is used when sorting and limiting data collections.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class HeapSelectionTest {

    /**
     * Simple functional test of [HeapSelection] with [Float] value.
     */
    @Test
    fun testMinWithSingleFloat() {
        val selection = HeapSelection(1, Comparator<Float> { o1, o2 -> o1.compareTo(o2) })
        val random = SplittableRandom()
        var min = Float.MAX_VALUE
        repeat(100000) {
            val value = random.nextFloat()
            selection.offer(value)
            min = min(value, min)
        }
        Assertions.assertTrue(selection.get(0) == min)
    }

    /**
     * Simple functional test of [HeapSelection] with [Float] value.
     */
    @Test
    fun testMaxWithSingleFloat() {
        val selection = HeapSelection(1, Comparator<Float> { o1, o2 -> -o1.compareTo(o2) })
        val random = SplittableRandom()
        var max = Float.MIN_VALUE
        repeat(100000) {
            val value = random.nextFloat()
            selection.offer(value)
            max = max(value, max)
        }
        Assertions.assertTrue(selection.get(0) == max)
    }

    /**
     * Simple functional test of [HeapSelection] with [Int] values.
     */
    @Test
    fun testWithIntAscending() {
        val selection = HeapSelection(100, Comparator<Int> { o1, o2 -> o1.compareTo(o2) })
        (0 until 100000).toList().shuffled().forEach { selection.offer(it) }
        for (i in 0 until 100) {
            Assertions.assertTrue(selection[i] == i)
        }
    }

    /**
     * Simple functional test of [HeapSelection] with [Int] values.
     */
    @Test
    fun testWithIntDescending() {
        val selection = HeapSelection(100, Comparator<Int> { o1, o2 -> -o1.compareTo(o2) })
        (0 until 100000).toList().shuffled().forEach { selection.offer(it) }
        for (i in 0 until 100) {
            Assertions.assertTrue(selection[i] == 99999 - i)
        }
    }
}