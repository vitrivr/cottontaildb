package org.vitrivr.cottontail

import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.json.DecodeSequenceMode
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeToSequence
import kotlinx.serialization.json.encodeToStream
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.*

/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */

fun main(args: Array<String>) {
    val path = Paths.get("/Users/rgasser/Downloads/package/warren.VBSLHE.descriptor_file.0.json")
    val json = Json { prettyPrint = true }
    Files.newInputStream(path, StandardOpenOption.READ).use { input ->
        val buffer = LinkedList<FileDescriptor>()
        var index = 0
        val descriptors = Json.decodeToSequence(input, FileDescriptor.serializer(), DecodeSequenceMode.ARRAY_WRAPPED)
        for (descriptor in descriptors) {
            buffer.add(descriptor)
            if (buffer.size == 50_000) {
                write(index++, buffer)
                buffer.clear()
            }
        }
        write(index, buffer)
    }
}

/**
 *
 */
fun write(index: Int, buffer: LinkedList<FileDescriptor>) {
    val path = Paths.get("/Users/rgasser/Downloads/package/out/warren.VBSLHE.descriptor_file.$index.json")
    Files.newOutputStream(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE).use { output ->
        Json.encodeToStream(ListSerializer(FileDescriptor.serializer()), buffer, output)
    }
}

@Serializable
data class Descriptor(val descriptorId: String, val retrievableId: String, val descriptor: FloatArray)

@Serializable
data class Retrievable(val retrievableId: String, val type: String)

@Serializable
data class Relationship(val objectId: String, val subjectId: String, val predicate: String)

@Serializable
data class FileDescriptor(val descriptorId: String = UUID.randomUUID().toString(), val retrievableId: String, val size: Int, val path: String) {}

@Serializable
data class TimeDescriptor(val descriptorId: String, val retrievableId: String, val start: Long, val end: Long)