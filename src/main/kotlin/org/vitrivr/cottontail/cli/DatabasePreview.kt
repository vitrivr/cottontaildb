package org.vitrivr.cottontail.cli

import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.grpc.CottonDDLGrpc
import org.vitrivr.cottontail.grpc.CottonDMLGrpc
import org.vitrivr.cottontail.grpc.CottonDQLGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc


object DatabasePreview {


    val channel = ManagedChannelBuilder.forAddress("localhost", 1865).usePlaintext().build()

    val dqlService = CottonDQLGrpc.newBlockingStub(channel)
    val ddlService = CottonDDLGrpc.newBlockingStub(channel)
    val dmlService = CottonDMLGrpc.newBlockingStub(channel)

    val schema = CottontailGrpc.Schema.newBuilder().setName("test").build()
    val entity = CottontailGrpc.Entity.newBuilder()
            .setSchema(schema)
            .setName("surf")
            .build()


    @JvmStatic
    fun main(args: Array<String>) {

        loop@ while (true) {
            try {
                println("Please enter a command. help gives an overview, quit also exists")
                val command = readLine()!!
                val input = command.split(" ")
                when (input[0]) {
                    "optimize" -> {
                        if (input.size < 3) {
                            println("optimize syntax is: optimize <schema> <entity>")
                            continue@loop
                        }
                        optimizeEntity(input[1], input[2])
                    }
                    "preview" -> {  //preview schema entity [count]
                        if (input.size < 3) {
                            println("preview syntax is: preview <schema> <entity> [<count>]")
                            continue@loop
                        }
                        if (input.size == 4) {
                            previewEntity(input[1], input[2], input[3].toLong())
                        } else {
                            previewEntity(input[1], input[2])
                        }
                    }
                    "count" -> {  //preview schema entity [count]
                        if (input.size < 3) {
                            println("count syntax is: count <schema> <entity> [<count>]")
                            continue@loop
                        }
                        countEntity(input[1], input[2])
                    }
                    "query" -> {
                        if (input.size < 5) {
                            println("query syntax is: query <schema> <entity> <column> <value>")
                            continue@loop
                        }
                        queryEntity(input[1], input[2], input[3], input[4])
                    }
                    "list" -> {
                        listEntities()
                    }
                    "drop" -> {
                        if(input.size < 3){
                            println("drop syntax is: drop <schema> <entity>")
                            continue@loop
                        }
                        dropEntity(input[1], input[2] )
                    }
                    "show" -> {
                        if(input.size < 3){
                            println("show syntax is: drop <schema> <entity>")
                            continue@loop
                        }
                        showEntity(input[1], input[2])
                    }
                    "quit" -> System.exit(1)
                    else -> printHelp()
                }
                println("fin")
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }
    }

    private fun queryEntity(schema: String, entity: String, col: String, value: String) {
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(From(Entity(entity, Schema(schema))))
                        .setProjection(MatchAll())
                        .setWhere(Where(
                                CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
                                        .setAttribute(col)
                                        .setOp(CottontailGrpc.AtomicLiteralBooleanPredicate.Operator.EQUAL)
                                        .addData(CottontailGrpc.Data.newBuilder().setStringData(value).build())
                                        .build()
                        ))
        ).build()
        val results = this.dqlService.query(qm)
        results.forEach { result ->
            if (result.tuple != null) {
                println(result.tuple)
            }
        }
    }

    private fun printHelp() {
        println("Available commands include help, preview, quit")
    }

    private fun listEntities() {
        println("Listing all Entities")
        ddlService.listSchemas(CottontailGrpc.Empty.getDefaultInstance()).forEach { _schema ->
            println("Entities for Schema ${_schema.name}:")
            ddlService.listEntities(_schema).forEach { _entity ->
                if (_entity.schema.name != _schema.name) {
                    println("Data integrity threat! entity $_entity ist returned when listing entities for schema $_schema")
                }
                println("${_schema.name} - ${_entity.name}")
            }
        }
    }


    private fun countEntity(schema: String, entity: String) {
        println("Counting elements of entity $schema.$entity")
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(From(Entity(entity, Schema(schema))))
                        .setProjection(CottontailGrpc.Projection.newBuilder().setOp(CottontailGrpc.Projection.Operation.COUNT).build())

        ).build()
        val results = dqlService.query(qm)
        results.forEach { result ->
            if (result.tuple != null) {
                println(result.tuple)
            }
        }
    }

    private fun previewEntity(schema: String, entity: String, limit: Long = 10) {
        println("Showing first $limit elements of entity $schema.$entity")
        val qm = CottontailGrpc.QueryMessage.newBuilder().setQuery(
                CottontailGrpc.Query.newBuilder()
                        .setFrom(From(Entity(entity, Schema(schema))))
                        .setProjection(MatchAll())
                        .setLimit(limit)
        ).build()
        val results = dqlService.query(qm)
        println("Previewing $limit elements of $entity")
        results.forEach { result ->
            if (result.tuple != null) {
                println(result.tuple)
            }
        }
    }

    private fun optimizeEntity(schema: String, entity: String) {
        println("Optimizing entity $schema.$entity")
        ddlService.optimizeEntity(Entity(entity, Schema(schema)))
        println("Done!")
    }

    private fun dropEntity(schema: String, entity: String){
        println("Dropping entity $schema.$entity")
        ddlService.dropEntity(Entity(entity, Schema(schema)))
        println("Done!")
    }

    private fun showEntity(schema:String, entity:String){
        val details = ddlService.entityDetails(Entity(entity, Schema(schema)))
        println("Entity ${details.entity.schema.name}.${details.entity.name} with ${details.columnsCount} columns: ")
        print("  ")
        details.columnsList.forEach { print("${it.name} (${it.type}), ") }
        println("")
    }
}
