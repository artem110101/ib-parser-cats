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

/* ------------------------------------------------------------------------------------------------------------------ */
/*  SQL helpers                                                                                                       */
/* ------------------------------------------------------------------------------------------------------------------ */

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

  sql"""
        INSERT INTO ticks
          (con_id, date, ticker, metric, timeframe, open, high, low, close,
           volume, "barCount", unique_id, event_time, source)
        VALUES $enc
     """.command
}

/* ------------------------------------------------------------------------------------------------------------------ */
/*  Runtime helpers                                                                                                   */
/* ------------------------------------------------------------------------------------------------------------------ */

/** Wait for every Promise already present in the map to complete.
  * A snapshot is taken first and each Future is awaited **in parallel**.
  */
def awaitAllPromises(historicalPromises: TrieMap[Int, Promise[Unit]]): IO[Unit] =
  historicalPromises
    .values
    .toList                       // snapshot
    .parTraverse_(p => IO.fromFuture(IO(p.future)))

/** Issue historical data requests for a fixed set of indices. */
def indicesHistoric(wrapper: MyWrapper): IO[Unit] = {
  /* ----------------------- contracts we are interested in ----------------------- */
  val contracts: List[Contract] =
    List(
      "VIX"   -> "CBOE",
      "V2TX"  -> "EUREX",
      "ESTX50"-> "EUREX",
      "SX7E"  -> "EUREX",
      "XSP"   -> "CBOE",
      "SPX"   -> "CBOE"
    ).map { case (symbol, exchange) =>
      val c = Contract()
      c.symbol(symbol)
      c.secType("IND")
      c.exchange(exchange)
      c
    }

  /* ----------------------- request parameters ---------------------------------- */
  val dataTypes  = List("TRADES", "OPTION_IMPLIED_VOLATILITY")
  val timeframe  = "1 week"
  val duration   = "5 Y"

  /* ----------------------- fire the requests ----------------------------------- */
  contracts.traverse_ { contract =>
    dataTypes.traverse_ { dataType =>
      IO.delay {
        val reqId = wrapper.reqIdCounter.incrementAndGet()

        // bookkeeping so that responses can be correlated
        wrapper.reqIdToContract.put(reqId, (contract, dataType, timeframe))
        wrapper.historicalPromises.put(reqId, Promise[Unit]())

        // actual network request
        wrapper.eClientSocket.reqHistoricalData(
          reqId,
          contract,
          "",          // now
          duration,
          timeframe,
          dataType,
          1,           // RTH only
          1,           // human-readable dates
          false,       // do not keep up to date
          null
        )
      }
    }
  }
}
