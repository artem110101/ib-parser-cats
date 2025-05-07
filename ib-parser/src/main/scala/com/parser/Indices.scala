package com.parser

import cats.effect.*
import cats.effect.implicits.*
import cats.effect.std.Console
import cats.effect.unsafe.IORuntime
import cats.implicits.*
import cats.syntax.all.*
import com.ib.client.*
import fs2.*
import fs2.Stream.resource
import fs2.io.net.Network
import natchez.Trace.Implicits.noop
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

import java.time.{LocalDate, LocalDateTime, OffsetDateTime, ZoneId, ZoneOffset, ZonedDateTime}
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.TemporalQueries
import java.util.UUID
import java.util.Properties
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}

/**
 * Entrypoint for gathering index data from Interactive Brokers and storing it
 * into Postgres.
 *
 * The logic is intentionally kept simple:
 *  1. Connect to the IB API.
 *  2. Request historical data for a predefined set of indices (`indicesHistoric`).
 *  3. Once the data has been received (we wait a fixed amount of time – 60 s),
 *     we persist everything to Postgres.
 *  4. Disconnect.
 *
 * NOTE: This is *not* production-grade – e.g. it uses blocking waits and the
 *       IB timeout is hard-coded. However it is sufficient for an MVP and is
 *       deliberately kept “low-touch” as requested.
 */
object Indices extends IOApp {

  // ---------------------------------------------------------------------------
  // Program entry-point
  // ---------------------------------------------------------------------------

  override def run(args: List[String]): IO[ExitCode] = {
    val histPromise = Promise[Unit]()
    val ibWrapper   = new MyWrapper(histPromise)

    for {
      // Load DB configuration (non-blocking)
      cfg                 <- loadDbConfig
      (host, port, user,
       database, pwdOpt)  = cfg

      // Construct Skunk session resource using the loaded config
      dbSession = Session.single(
        host     = host,
        port     = port,
        user     = user,
        database = database,
        password = pwdOpt,      // may be None if the env-var is not set
        debug    = false
      )

      // 1) Connect to IB
      _ <- connectToIb(ibWrapper)

      // 2) Request historical data
      _ <- indicesHistoric(ibWrapper)

      // 3) Wait for the data to arrive – quick & dirty
      _ <- IO.sleep(60.seconds)

      // 4) Persist everything to Postgres
      _ <- persistBars(ibWrapper, dbSession)

      // 5) Disconnect
      _ <- IO(ibWrapper.eClientSocket.eDisconnect())
    } yield ExitCode.Success
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Establishes a connection to the IB Gateway / TWS */
  private def connectToIb(wrapper: MyWrapper): IO[Unit] =
    for {
      _         <- IO(wrapper.connect("172.26.16.1", 7496, 1))
      _         <- IO(wrapper.eClientSocket.reqMarketDataType(3))              // Delayed-frozen data
      connected <- IO(wrapper.eClientSocket.isConnected)
      _ <-
        if (connected) IO.println("Connected to Interactive Brokers API")
        else IO.raiseError(new Exception("Could not connect to Interactive Brokers API"))
      // Give the socket some breathing room
      _ <- IO.sleep(5.seconds)
    } yield ()

  /**
   * Reads `db.conf` from the classpath and combines it with the `DB_PASSWORD`
   * environment variable (if present).  The password is therefore never stored
   * in Git.
   */
  private def loadDbConfig: IO[(String, Int, String, String, Option[String])] = IO.blocking {
    val props = new Properties()
    Option(getClass.getClassLoader.getResourceAsStream("db.conf")).foreach(props.load)

    val host     = props.getProperty("host", "127.0.0.1")
    val port     = props.getProperty("port", "5432").toInt
    val user     = props.getProperty("user", "postgres")
    val database = props.getProperty("database", "postgres")
    val pwdOpt   = Option(System.getenv("DB_PASSWORD"))

    (host, port, user, database, pwdOpt)
  }

  /** Flattens the in-memory bar cache of [[MyWrapper]] and writes it to Postgres */
  private def persistBars(
      wrapper:    MyWrapper,
      dbSessionR: Resource[IO, Session[IO]]
  ): IO[Unit] = {
    val uniqueId = UUID.randomUUID()
    val now      = LocalDateTime.now()

    // Flatten the nested map structure into a simple `List[BarInsert]`
    val flattened: List[BarInsert] =
      wrapper.barAllData.flatMap { case (contractKey, innerMap) =>
        innerMap.flatMap { case (metric, barDataVec) =>
          barDataVec.map { bd =>
            BarInsert(
              conId      = contractKey.contract.conid(),
              time       = convertToUtc(bd.bar.time()),
              ticker     = bd.symbol,
              metric     = metric,
              timeframe  = bd.timeframe,
              open       = bd.bar.open(),
              high       = bd.bar.high(),
              low        = bd.bar.low(),
              close      = bd.bar.close(),
              volume     = bd.bar.volume().value(),
              barCount   = bd.bar.count(),
              unique_id  = uniqueId,
              event_time = now,
              source     = "indices"
            )
          }
        }
      }.toList

    // Persist in batches to avoid huge INSERTs
    val insertAll: IO[Unit] = dbSessionR.use { sess =>
      flattened
        .grouped(1024)
        .toList
        .traverse_ { chunk =>
          sess.prepare(insertManyBars(chunk)).flatMap(_.execute(chunk)).void
        }
    }

    insertAll
  }

  /**
   * Converts IB’s bar timestamp (which may or may not include a time
   * component) to UTC.
   *
   * Example inputs:
   *   - "20230922 23:45:00 Europe/Berlin"
   *   - "20230922"
   */
  private def convertToUtc(dateTimeStr: String): OffsetDateTime = {
    val withTimeFmt  = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV")
    val dateOnlyFmt  = DateTimeFormatter.ofPattern("yyyyMMdd")

    def parseWith(formatter: DateTimeFormatter): OffsetDateTime =
      ZonedDateTime
        .from(formatter.parse(dateTimeStr))
        .toOffsetDateTime
        .withOffsetSameInstant(ZoneOffset.UTC)

    try parseWith(withTimeFmt)
    catch {
      case _: DateTimeParseException =>
        val localDate =
          dateOnlyFmt.parse(dateTimeStr).query(TemporalQueries.localDate())
        localDate
          .atStartOfDay(ZoneId.systemDefault())
          .toOffsetDateTime
          .withOffsetSameInstant(ZoneOffset.UTC)
    }
  }
}
