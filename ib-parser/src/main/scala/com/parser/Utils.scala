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

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util.Calendar
import java.util.concurrent.atomic.AtomicInteger
import java.util.Locale
import java.{lang, util}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success}



def insertManyBars(ps: List[BarInsert]) = {
  val enc = (
    int8 *:
      timestamptz *:
      varchar *:
      varchar *:
      varchar *:
      numeric *:
      numeric *:
      numeric *:
      numeric *:
      numeric *:
      int8 *:
      uuid *:
      timestamp *:
      varchar
    ).to[BarInsert].values.list(ps)
  sql"""INSERT INTO ticks (con_id, date, ticker, metric, timeframe, open, high, low, close, volume, "barCount", unique_id, event_time, source) VALUES $enc""".command
}

def awaitAllPromises(historicalPromises: TrieMap[Int, Promise[Unit]]): IO[Unit] = {
  val futures = historicalPromises.values.toList.map { promise =>
    IO.fromFuture(IO(promise.future))
  }

  futures.sequence_.void
}

def indicesHistoric(wrapper: MyWrapper): IO[Unit] = IO {
  val results = ArrayBuffer[Int]()

  val contract1 = Contract()
  contract1.symbol("VIX")
  contract1.secType("IND")
  contract1.exchange("CBOE")

  val contract2 = Contract()
  contract2.symbol("V2TX")
  contract2.secType("IND")
  contract2.exchange("EUREX")

  val contract3 = Contract()
  contract3.symbol("ESTX50")
  contract3.secType("IND")
  contract3.exchange("EUREX")

  val contract4 = Contract()
  contract4.symbol("SX7E")
  contract4.secType("IND")
  contract4.exchange("EUREX")

  val contract5 = Contract()
  contract5.symbol("XSP")
  contract5.secType("IND")
  contract5.exchange("CBOE")

  val contract6 = Contract()
  contract6.symbol("SPX")
  contract6.secType("IND")
  contract6.exchange("CBOE")

  // CAC40
  // V1X - germany

  val contracts = List(contract1, contract2, contract3, contract4, contract5, contract6)

  for (contract <- contracts) {
    // for (dataType <- List("ADJUSTED_LAST", "OPTION_IMPLIED_VOLATILITY")) {
    for (dataType <- List("TRADES", "OPTION_IMPLIED_VOLATILITY")) {
      val histReqId = wrapper.reqIdCounter.incrementAndGet()

      val timeframe = "1 week"
      val duration = "5 Y"

      wrapper.reqIdToContract.put(
        histReqId,
        (contract, dataType, timeframe)
      ) // Store the link between request ID and contract

      wrapper.historicalPromises.put(histReqId, Promise[Unit]())

      // Request historical data
      wrapper.eClientSocket.reqHistoricalData(
        histReqId, // tickerId
        contract, // contract
        "", // endDateTime
        duration, // durationString
        timeframe, // barSizeSetting
        // "ADJUSTED_LAST", // whatToShow
        dataType,
        1, // useRTH, Whether (1) or not (0) to retrieve data generated only within Regular Trading Hours (RTH)
        1, // formatDate
        false, // keepUpToDate
        null
      )

      results += histReqId
    }
  }

  results
}
