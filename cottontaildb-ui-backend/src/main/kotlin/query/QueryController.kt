package query

import ClientConfig
import io.javalin.http.Context
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query


object QueryController {

    class Query (){

    }

    fun query(context: Context)  {

        var client = ClientConfig(context.pathParam("port").toInt()).client
        println(context.queryParam("FROM"))
        println(context.queryParams("SELECT"))

        var query = Query(context.queryParam("FROM"))
        for(column in context.queryParams("SELECT")){
            query = query.select(column)
        }
        val result = client.query(query)
        context.json(result)

        //return cache.get(Triple(context.req().session.id, queryMessage, client))



    }

    fun executeQuery(client: SimpleClient, query: Query) {

    }



}