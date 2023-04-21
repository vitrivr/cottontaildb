package org.vitrivr.cottontail.ui.api.schema

import initClient
import io.javalin.http.Context
import org.vitrivr.cottontail.client.iterators.TupleIterator
import org.vitrivr.cottontail.client.language.ddl.CreateSchema
import org.vitrivr.cottontail.client.language.ddl.DropSchema
import org.vitrivr.cottontail.client.language.ddl.ListSchemas
import org.vitrivr.cottontail.grpc.CottontailGrpc

object SchemaController {

    fun createSchema(context: Context){
        val client = initClient(context)
        val schemaName = context.pathParam("name")
        println(schemaName)
        val result : TupleIterator = client.create(CreateSchema(schemaName))
        context.status(201)
    }
    fun dropSchema(context: Context){
        val client = initClient(context)
        val schemaName = context.pathParam("name")
        val result : TupleIterator = client.drop(DropSchema(schemaName))
        context.status(200)
    }
    fun dumpSchema(context: Context){
        TODO()
    }

    fun listAllSchemas(context: Context) {
        val client = initClient(context)
        /** using ClientConfig's client, sending ListSchemas message to cottontaildb*/
        val result: TupleIterator = client.list(ListSchemas())
        val schemas: MutableList<Schema> = mutableListOf()
        /** iterate through schemas*/
        result.forEach {
            if (it.asString(0).isNullOrBlank()){
                context.json({})
            } else {
                //it.asString(0) is nullable
                schemas.add(Schema(it[0].toString()))
            }
            /** first value of tuple is the name */
            /**val schemaName = itSchema.asString(0)*/
        }
        context.json(schemas)
    }

    fun listEntities(context: Context){
        val client = initClient(context)
        val schemaName = context.pathParam("name")

        val result: TupleIterator = client
            .list(
                CottontailGrpc.ListEntityMessage.newBuilder().setSchema(
                    CottontailGrpc.SchemaName.newBuilder()
                        .setName(schemaName).build()).build())

        val entities: MutableList<String?> = mutableListOf()
        /** iterate through entities*/
        result.forEach { it ->
            entities.add(it.asString(0))
        }
        context.json(entities)
    }


}