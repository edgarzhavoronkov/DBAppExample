import com.zaxxer.hikari.HikariDataSource
import fi.iki.elonen.NanoHTTPD

class ServerWithPool: NanoHTTPD("localhost", 9010) {
  private val dataSource = HikariDataSource()

  init {
    dataSource.jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
    dataSource.username = "postgres"
    dataSource.password = "foobar"
  }

  override fun serve(session: IHTTPSession): Response {
    return when (session.uri) {
      "/planets" -> {
        val dbConn = dataSource.connection
        val planets = mutableListOf<String>()
        println(session.parameters)
        if (session.parameters["planet_id"] == null) {
          dbConn.use {
            val stmt = it.prepareStatement("SELECT * FROM Planet")
            val resultSet = stmt.executeQuery()
            while (resultSet.next()) {
              val planet = resultSet.getString("name")
              planets.add(planet)
            }
          }
        } else {
          val planetId = session.parameters["planet_id"]?.get(0)
          dbConn.use {
            val stmt = it.prepareStatement("SELECT * FROM Planet P WHERE P.id = ?")
            stmt.setInt(1, Integer.valueOf(planetId))
            val resultSet = stmt.executeQuery()
            while (resultSet.next()) {
              val planet = resultSet.getString("name")
              planets.add(planet)
            }
          }
        }
        return newFixedLengthResponse(planets.joinToString("\n"))
      }
      else -> newFixedLengthResponse("Hey there!")
    }
  }
}
