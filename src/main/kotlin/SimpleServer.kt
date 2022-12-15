import fi.iki.elonen.NanoHTTPD
import java.sql.DriverManager

class SimpleServer: NanoHTTPD("localhost", 9010) {
  private val dbUrl = "jdbc:postgresql://localhost:5432/postgres"

  override fun serve(session: IHTTPSession): Response {
    return when (session.uri) {
      "/planets" -> {
        val dbConn = DriverManager.getConnection(dbUrl, "postgres", "foobar")
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
            //stmt.setParameter("planet_id", planetId)
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
