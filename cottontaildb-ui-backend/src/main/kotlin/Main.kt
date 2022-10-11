import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.*

fun main() {


    val app = Javalin.create(/*config*/)
        .start(7070)


    app.routes {
        path("/list") {
            get(TestController::getList)
        }

    }
}