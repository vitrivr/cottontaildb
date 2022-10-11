import io.javalin.Javalin
import io.javalin.apibuilder.ApiBuilder.*
import io.javalin.http.Header

fun main() {


    val app = Javalin.create {
    }.start(7070)


    app.routes {
        before { ctx ->
            ctx.header(Header.ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        }
        path("/list") {
            get(TestController::getList)
        }
    }


}