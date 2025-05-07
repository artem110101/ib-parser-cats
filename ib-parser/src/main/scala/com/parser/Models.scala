package com.parser

import com.ib.client.*

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, OffsetDateTime}
import java.util.Calendar
import java.util.concurrent.atomic.AtomicInteger
import java.{lang, util}
import scala.collection.concurrent.TrieMap
import java.util.UUID


final case class ContractKey(contract: Contract) {
  override def equals(obj: Any): Boolean = obj match {
    case ContractKey(c) => contract.conid() == c.conid() && contract.symbol() == c.symbol() // Add additional comparisons as needed
    case _ => false
  }

  override def hashCode(): Int = contract.conid() // Or some other unique identifier for the contract
}

final case class Stock(
                        con_id: Long,
                        symbol: String,
                        exchange: String,
                        currency: String,
                        local_symbol: String,
                        trading_class: String,
                        primary_exchange: String,
                        industry: String,
                        category: String,
                        subcategory: String,
                        long_name: String,
                        time: LocalDateTime,
                        close: BigDecimal,
                        put_open_interest: Long,
                        call_open_interest: Long,
                        hist_volatility: Double,
                        implied_volatility: Double,
                        dividends_past_12months: Double,
                        dividends_next_12months: Double,
                        dividends_next_date: LocalDate,
                        dividends_next_amount: Double,
                        avg_opt_volume: Long,
                        volume: Long,
                        option_put_volume: Long,
                        option_call_volume: Long,
                        xml: String,
                        unique_id: UUID,
                        event_time: LocalDateTime
                      )

final case class BarInsert(
                            conId: Long,
                            time: OffsetDateTime,
                            ticker: String,
                            metric: String,
                            timeframe: String,
                            open: BigDecimal,
                            high: BigDecimal,
                            low: BigDecimal,
                            close: BigDecimal,
                            volume: BigDecimal,
                            barCount: Long,
                            unique_id: UUID,
                            event_time: LocalDateTime,
                            source: String
                          )

final case class BarData(
                          reqId: Int,
                          symbol: String,
                          timeframe: String,
                          bar: Bar
                        )
