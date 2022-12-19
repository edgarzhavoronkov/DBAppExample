import com.zaxxer.hikari.HikariDataSource
import fi.iki.elonen.NanoHTTPD
import fi.iki.elonen.NanoHTTPD.Response.IStatus
import fi.iki.elonen.NanoHTTPD.Response.Status
import java.sql.Connection
import java.sql.Date
import java.sql.DriverManager

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
      "/set_distance" -> {
        val planetId = session.parameters["planet_id"]?.get(0)?.toLong()
        val distance = session.parameters["distance"]?.get(0)?.toLong()
        val date = session.parameters["date"]?.get(0)

        if (planetId != null && distance != null && date != null) {
          val planet = Planet(planetId)
          val isOk = planet.setDistance(distance, date)
          if (!isOk) {
            return newFixedLengthResponse(Status.BAD_REQUEST, MIME_PLAINTEXT, "")
          }
        } else {
          return newFixedLengthResponse(Status.NOT_FOUND, MIME_PLAINTEXT, "Requested planet is not found")
        }

        return newFixedLengthResponse("Hey there!")
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

  fun setDistance(distance: Long, flightDate: String): Boolean {
    return txn { dbConn -> setDistanceWorker(dbConn, distance, flightDate) }
    /*val dbConn = dataSource.connection
    dbConn.use {
      var stmt = dbConn.prepareStatement("UPDATE flightrecords SET distance=? WHERE planet_id=? AND date=?")
      stmt.setLong(1, distance)
      stmt.setLong(2, id)
      stmt.setDate(3, Date.valueOf(flightDate))
      var rowCount = stmt.executeUpdate()
      if (rowCount > 0) {
        dbConn.commit()
        return true
      }

      stmt = dbConn.prepareStatement("SELECT MAX(id) FROM flightrecords")
      val resultSet = stmt.executeQuery()
      resultSet.next()
      val maxId = resultSet.getLong("max")

      stmt = dbConn.prepareStatement("INSERT INTO flightrecords(id, planet_id, date) VALUES (?, ?, ?)")
      stmt.setLong(1, maxId + 1)
      stmt.setLong(2, id)
      stmt.setDate(3, Date.valueOf(flightDate))
      rowCount = stmt.executeUpdate()
      if (rowCount == 0) {
        return false
      }

      stmt = dbConn.prepareStatement("UPDATE flightrecords SET distance=? WHERE planet_id=? AND date=?")
      stmt.setLong(1, distance)
      stmt.setLong(2, id)
      stmt.setDate(3, Date.valueOf(flightDate))
      val result = stmt.executeUpdate() > 0
      dbConn.commit()
      return result
    }*/
  }

  private fun setDistanceWorker(dbConn: Connection, distance: Long, flightDate: String): Boolean {
    var stmt = dbConn.prepareStatement("UPDATE flightrecords SET distance=? WHERE planet_id=? AND date=?")
    stmt.setLong(1, distance)
    stmt.setLong(2, id)
    stmt.setDate(3, Date.valueOf(flightDate))
    var rowCount = stmt.executeUpdate()
    if (rowCount > 0) {
      return true
    }

    stmt = dbConn.prepareStatement("SELECT MAX(id) FROM flightrecords")
    val resultSet = stmt.executeQuery()
    resultSet.next()
    val maxId = resultSet.getLong("max")

    stmt = dbConn.prepareStatement("INSERT INTO flightrecords(id, planet_id, date) VALUES (?, ?, ?)")
    stmt.setLong(1, maxId + 1)
    stmt.setLong(2, id)
    stmt.setDate(3, Date.valueOf(flightDate))
    rowCount = stmt.executeUpdate()
    if (rowCount == 0) {
      return false
    }

    stmt = dbConn.prepareStatement("UPDATE flightrecords SET distance=? WHERE planet_id=? AND date=?")
    stmt.setLong(1, distance)
    stmt.setLong(2, id)
    stmt.setDate(3, Date.valueOf(flightDate))
    val result = stmt.executeUpdate() > 0
    return result
  }

  private fun<T> txn(work: (Connection) -> T): T {
    val dbConnection = dataSource.connection
    dbConnection.use {
      val result = work(dbConnection)
      dbConnection.commit()
      return result
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

val dbUrl = "jdbc:postgresql://localhost:5432/postgres"

val dataSource = HikariDataSource().also {
  it.jdbcUrl = dbUrl
  it.username = "postgres"
  it.password = "foobar"
  it.isAutoCommit = false
}
