import com.zaxxer.hikari.HikariDataSource
import fi.iki.elonen.NanoHTTPD

class ComplexServer: NanoHTTPD("localhost", 9010) {
  override fun serve(session: IHTTPSession): Response {
    return when (session.uri) {
      "/planets" -> {
        val planetId = session.parameters["planet_id"]?.get(0)?.toLong()
        val planets = allPlanets()
          .map(::Planet)
          .filter { planet ->
            planetId == null || planet.id == planetId
          }
          return newFixedLengthResponse(planets.joinToString(
            separator = "\n",
            transform = {
                planet -> "Name: ${planet.name()}, distance: ${planet.distance()}, flights count: ${planet.flightCount()}"
            }
          ))
      }
      else -> {
        newFixedLengthResponse("Hey there!")
      }
    }
  }

  private fun allPlanets(): List<Long> {
    val dbConn = dataSource.connection
    dbConn.use {
      val stmt = it.prepareStatement("SELECT id FROM Planet")
      val ids = mutableListOf<Long>()
      val resultSet = stmt.executeQuery()
      while (resultSet.next()) {
        val planetId = resultSet.getLong("id")
        ids.add(planetId)
      }
      return ids
    }
  }
}

class Planet(val id: Long) {
  fun name(): String? {
    val dbConn = dataSource.connection
    dbConn.use {
      val stmt = it.prepareStatement("SELECT name FROM Planet P WHERE P.id=?")
      stmt.setLong(1, id)
      val resultSet = stmt.executeQuery()
      resultSet.next()
      return resultSet.getString("name")
    }
  }

  fun distance(): Long {
    val dbConn = dataSource.connection
    dbConn.use {
      val stmt = it.prepareStatement("SELECT distance FROM Planet P WHERE P.id=?")
      stmt.setLong(1, id)
      val resultSet = stmt.executeQuery()
      resultSet.next()
      return resultSet.getLong("distance")
    }
  }

  private fun flights(): List<Long> {
    val dbConn = dataSource.connection
    dbConn.use {
      val stmt = it.prepareStatement("SELECT id FROM Flight F WHERE F.planet_id=?")
      stmt.setLong(1, id)
      val resultSet = stmt.executeQuery()
      val flightIds = mutableListOf<Long>()
      while (resultSet.next()) {
        val flightId = resultSet.getLong("id")
        flightIds.add(flightId)
      }
      return flightIds
    }
  }

  fun flightCount(): Int {
    return flights().size
  }
}

val dataSource = HikariDataSource().also {
  it.jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
  it.username = "postgres"
  it.password = "foobar"
}
