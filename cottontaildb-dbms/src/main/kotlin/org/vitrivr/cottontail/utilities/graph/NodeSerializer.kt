package org.vitrivr.cottontail.utilities.graph

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.util.LightOutputStream
import java.io.ByteArrayInputStream

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface NodeSerializer<V> {


    fun serialize(vertex: V): ByteIterable

    /**
     *
     */
    fun write(vertex: V, output: LightOutputStream)

    /**
     *
     */
    fun read(input: ByteArrayInputStream): V

    fun deserialize(iterable: ByteIterable): V
}