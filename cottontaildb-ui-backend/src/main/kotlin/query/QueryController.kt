package query

import ClientConfig
import cache
import io.javalin.http.Context
import org.vitrivr.cottontail.client.language.dql.Query


object QueryController {

    class QueryData(val columnNames: List<String>, val rows: MutableList<List<String>>){
        fun paginate(size: Int) : List<Page>{
            val chunks = rows.chunked(size)
            val pages = mutableListOf<Page>()
            repeat(chunks.size){
                val page = Page(columnNames, chunks[it])
                pages.add(page)
            }
            return pages
        }
    }

    class Page(val columnNames: List<String>, val rows:List<List<String>>)


    fun query(context: Context)  {

        val pageSize = 10
        val page = 0

        val port = context.pathParam("port").toInt()
        val queryMap = context.queryParamMap().toMutableMap()
        // Remove pagination parameters -> queryMap.remove()
        val key = Triple(context.req().session.id, queryMap, port)

        println("key: ")
        print(key)

        context.json(cache.get(key).paginate(pageSize)[page])


    }

    fun executeQuery(queryMap: MutableMap<String, List<String>>, port: Int): QueryData {

        val client = ClientConfig(port).client
        var query = Query(queryMap["FROM"]?.elementAt(0))

        val select = queryMap["SELECT"]
        if (select != null) {
            for (column in select) {
                query = query.select(column)
            }
        }

        val result =  client.query(query)

        val tuples : MutableList<List<String>> = mutableListOf()
        result.forEach {tuple ->
            val list = mutableListOf<String>()
            repeat(tuple.size()){
                list.add(tuple[it].toString())
            }
            tuples.add(list)
        }

        return QueryData(result.columnNames, tuples)

    }



}