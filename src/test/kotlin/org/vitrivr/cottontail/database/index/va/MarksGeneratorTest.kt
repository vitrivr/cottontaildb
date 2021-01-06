package org.vitrivr.cottontail.database.index.va

import org.junit.jupiter.api.Test

import org.junit.jupiter.api.Assertions.*
import org.vitrivr.cottontail.database.index.va.marks.MarksGenerator
import org.vitrivr.cottontail.database.index.va.vaplus.VAPlus

internal class MarksGeneratorTest {

    val random = java.util.Random(1234)
    val realdata = Array(30) {
        DoubleArray(20) { random.nextGaussian() }
    }
    val marksPerDim = 10

    @Test
    fun getEquidistantMarks() {
        println("data")
        realdata.forEach { println(it.joinToString()) }
        println("marks")
        val marks = MarksGenerator.getEquidistantMarks(realdata, IntArray(realdata.first().size) { marksPerDim })
        for (m in marks.marks) {
            assertTrue(m.toList() == m.toList().sorted(), "Marks not sorted in ascending order!")
            println(m.joinToString())
        }
    }

    @Test
    fun getNonUniformMarks() {
        println("data")
        realdata.forEach { println(it.joinToString()) }
        println("marks (each dim a row)")
        val marks = MarksGenerator.getNonUniformMarks(realdata, IntArray(realdata.first().size) { marksPerDim })
        marks.marks.forEach { m ->
            assertTrue(m.toList() == m.toList().sorted(), "Marks not sorted in ascending order!")
            println(m.joinToString()) }
    }

    @Test
    fun getEquallyPopulatedMarks() {
        println("data")
        realdata.forEach { println(it.joinToString()) }
        println("marks (each dim a row)")
        val marks = MarksGenerator.getEquallyPopulatedMarks(realdata, IntArray(realdata.first().size) { marksPerDim })
        val vap = VAPlus()
        println("cells for vecs")
        val cells = realdata.map {
            val c = marks.getCells(it)
            println(c.joinToString())
            c
        }
        realdata.first().indices.map {dim ->
            println("dim $dim counts per cell in cell order")
            val map = (0 until marksPerDim - 1).map { cell ->
                cells.count { it[dim] == cell }
            }
            println(map.joinToString())
            assertTrue(map.max()!! - map.min()!! <= 1, "Dim $dim Difference between most and least per cell larger than 1! Not optimal!")
        }
        marks.marks.forEachIndexed { i, m ->
            assertTrue(m.toList() == m.toList().sorted(), "Marks not sorted in ascending order!")
            println(m.joinToString())

        }
    }
}