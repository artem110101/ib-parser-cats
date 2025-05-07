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
import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Calendar, Date, Locale, TimeZone, UUID}
import java.{lang, util}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}


object Indices extends IOApp {
  val session: Resource[IO, Session[IO]] =
    Session.single(
      // host = "148.113.182.46",
      host = "127.0.0.1",
      user = "postgres",
      database = "postgres",
      password = Some("70f5RVb0MDOc2OPps7s8t6dOky4kWR"),
      port = 5432,
      debug = false
    )

  def run(args: List[String]): IO[ExitCode] = {
    val histPromise = Promise[Unit]()
    val wrapper = new MyWrapper(
      histPromise
    )
    val uniqueId = UUID.randomUUID()
    val now = LocalDateTime.now()

    for {
      _ <- IO(wrapper.connect("172.26.16.1", 7496, 1))

      _ <- IO(wrapper.eClientSocket.reqMarketDataType(3))

      _ <- IO(wrapper.eClientSocket.isConnected).flatMap(connected =>
        if (connected)
          IO(println("Connected to Interactive Brokers API"))
        else
          IO.raiseError(new Exception("Could not connect to Interactive Brokers API"))
      )

      _ <- IO.sleep(5.seconds)

      _ <- indicesHistoric(wrapper)

      _ <- IO.sleep(60.seconds)

      _ <- session.use { session =>
        val flattenedData: List[BarInsert] = wrapper.barAllData.flatMap { case (contract, innerMap) =>
          innerMap.map {
            (metric, values) =>
              values.map { x =>
                BarInsert(
                  conId = contract.contract.conid(),
                  time = convertToUtc(x.bar.time()),
                  ticker = x.symbol,
                  metric = metric,
                  timeframe = x.timeframe,
                  open = x.bar.open(),
                  high = x.bar.high(),
                  low = x.bar.low(),
                  close = x.bar.close(),
                  volume = x.bar.volume().value(),
                  barCount = x.bar.count(),
                  unique_id = uniqueId,
                  event_time = now,
                  source = "indices"
                )
              }
          }.flatten
        }.toList

        val insertStatement = session.prepare(insertManyBars(flattenedData))

        flattenedData
          .grouped(1024)
          .toList
          .map {
            x =>
              val s = x.toList
              session.prepare(insertManyBars(s)).flatMap(_.execute(s))
          }.sequence_.void
      }

      _ <- IO(wrapper.eClientSocket.eDisconnect())

    } yield ExitCode.Success
  }

  def convertToUtc(dateTimeStr: String): OffsetDateTime = {
    val formatterWithTime = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV", Locale.US)
    val formatterDateOnly = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.US)

    try {
      val temporalAccessor = formatterWithTime.parse(dateTimeStr)
      val zdt = ZonedDateTime.from(temporalAccessor)
      val offsetDateTime = zdt.toOffsetDateTime
      offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC)
    } catch {
      case _: DateTimeParseException =>
        val temporalAccessor = formatterDateOnly.parse(dateTimeStr)
        val localDate = temporalAccessor.query(TemporalQueries.localDate())
        val zdt = localDate.atStartOfDay(ZoneId.systemDefault())
        val offsetDateTime = zdt.toOffsetDateTime
        offsetDateTime.withOffsetSameInstant(ZoneOffset.UTC)
    }
  }

}
