package query

import channelCache
import io.javalin.http.Context
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.dql.Query
import pagedCache
import queryCache

object QueryController {

    data class QueryPageKey (val sessionID: String,
                       val queryMap : Map<String, List<String>>,
                       val pageSize: Int,
                       val page: Int,
                       val port: Int)  {
    }

    data class QueryKey(val sessionID: String, val queryMap: Map<String, List<String>>, val port: Int)



    class QueryData(private val columnNames: List<String>, private val rows: MutableList<List<String>>){

        fun paginate(size: Int) : List<Page>{
            println("PAGINATING")
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

        println("QUERY CALL")
        val sessionId = context.req().session.id
        println(sessionId)

        val pageSize = 10
        val page = 0

        val port = context.pathParam("port").toInt()
        val queryMap = context.queryParamMap().toMutableMap()

        val key = QueryPageKey(sessionId, queryMap,pageSize,page,port)
        println(key)
        context.json(pagedCache.get(key))


    }

    fun executeQuery(queryMap: Map<String, List<String>>, port: Int): QueryData {

        println("EXECUTE")


        val channel = channelCache.get(Pair(port,"localhost"))
        val client = SimpleClient(channel)

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

        client.close()
        return QueryData(result.columnNames, tuples)

    }

    fun computePages(sessionID: String, queryMessage: Map<String, List<String>>, port: Int, pageSize: Int) : List<Page> {
        println("COMPUTE PAGES")
        return queryCache.get(QueryKey(sessionID,queryMessage,port)).paginate(pageSize)
    }


}