package com.parser

import com.ib.client.*

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicInteger
import java.{lang, util}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class MyWrapper(
                 histPromise: Promise[Unit]
               ) extends EWrapper {

  private val signal = new EJavaSignal()
  val eClientSocket = new EClientSocket(this, signal)
  private var reader: EReader = null

  private val contractDetailsBuffer = new ListBuffer[ContractDetails]()

  // historical bars
  val barAllData: TrieMap[ContractKey, Map[String, Vector[BarData]]] = TrieMap()

  var historicalPromises: TrieMap[Int, Promise[Unit]] = TrieMap()

  // tick data
  val reqData: TrieMap[ContractKey, TrieMap[String, Any]] = TrieMap()

  // map
  val reqIdToContract: TrieMap[Int, (Contract, String, String)] = TrieMap()
  val reqIdCounter: AtomicInteger = new AtomicInteger(0) // Global counter for request IDs

  var scanTmp: Promise[List[ContractDetails]] = null

  def connect(host: String, port: Int, clientId: Int)(implicit ec: ExecutionContext): Unit = {
    eClientSocket.eConnect(host, port, clientId)

    reader = new EReader(eClientSocket, signal)
    reader.start()

    if (eClientSocket.isConnected) {
      Future {
        while (eClientSocket.isConnected) {
          signal.waitForSignal()
          try {
            reader.processMsgs()
          } catch {
            case e: Exception => println("Exception here: " + e.getMessage)
          }
        }
      } onComplete {
        case Success(_) => println("Finished processing messages.")
        case Failure(e) => {
          println("An error has occured: " + e.getMessage)
          e.printStackTrace()
        }
      }
    }
  }

  override def contractDetails(reqId: Int, contractDetails: ContractDetails): Unit = {
    reqIdToContract.get(reqId).foreach { (contract, reqType, timeframe) =>
      val contractKey = ContractKey(contract) // Create a key from the contract

      val innerMap = reqData.getOrElseUpdate(contractKey, TrieMap())
      innerMap.put("conid", contractDetails.conid())
      innerMap.put("localSymbol", contractDetails.contract().localSymbol())
      innerMap.put("tradingClass", contractDetails.contract().tradingClass())
      innerMap.put("primaryExch", contractDetails.contract().primaryExch())
      innerMap.put("marketName", contractDetails.marketName())

      innerMap.put("longName", contractDetails.longName())
      innerMap.put("industry", contractDetails.industry())
      innerMap.put("category", contractDetails.category())
      innerMap.put("subcategory", contractDetails.subcategory())
    }
  }

  override def historicalData(reqId: Int, bar: Bar): Unit = {
    println(s"Received data: ${bar.time()} ${bar.close()}")

    reqIdToContract.get(reqId).foreach { (contract, reqType, timeframe) =>
      val contractKey = ContractKey(contract) // Create a key from the contract
      val barData = BarData(reqId, contract.symbol(), timeframe, bar)

      barAllData.updateWith(contractKey) {
        case Some(innerMap) =>
          val updatedInnerMap = innerMap.updated(reqType, innerMap.getOrElse(reqType, Vector.empty) :+ barData)
          Some(updatedInnerMap)
        case None =>
          val initialInnerMap = scala.collection.immutable.Map(reqType -> Vector(barData))
          Some(initialInnerMap)
      }
    }
  }

  override def historicalDataEnd(reqId: Int, startDateStr: String, endDateStr: String): Unit = {
    println(s"Finished receiving data for request $reqId")

    historicalPromises.get(reqId).foreach { promise =>
      promise.success(())
    }
  }

  override def error(e: Exception): Unit = {
    println("error")
    scanTmp.failure(e)
  }

  override def error(id: Int, errorTime: Long, errorCode: Int, errorMsg: String, advancedOrderRejectJson: String): Unit = {
    println(errorMsg)
    if (id == 1 && errorCode >= 2100 && errorCode <= 2110) { // handle error codes related to market data farm connection
      scanTmp.failure(new Exception(s"Error $errorCode: $errorMsg"))
    }

    if (id > 1 && historicalPromises.contains(id)) {
      historicalPromises.get(id).foreach { promise =>
        promise.success(())
      }
    }
  }

  override def tickPrice(tickerId: Int, field: Int, price: Double, attrib: TickAttrib): Unit = {
    if (TickType.get(field) == TickType.CLOSE) {
      reqIdToContract.get(tickerId).foreach { (contract, reqType, timeframe) =>
        val contractKey = ContractKey(contract) // Create a key from the contract

        val innerMap = reqData.getOrElseUpdate(contractKey, TrieMap())
        innerMap.put(TickType.get(field).field(), price)
      }
    }
  }

  override def tickSize(tickerId: Int, field: Int, size: Decimal): Unit = {
    reqIdToContract.get(tickerId).foreach { (contract, reqType, timeframe) =>
      val contractKey = ContractKey(contract) // Create a key from the contract

      val innerMap = reqData.getOrElseUpdate(contractKey, TrieMap())
      innerMap.put(TickType.get(field).field(), BigDecimal(size.value()))
    }
  }

  override def tickGeneric(tickerId: Int, tickType: Int, value: Double): Unit = {
    reqIdToContract.get(tickerId).foreach { (contract, reqType, timeframe) =>
      val contractKey = ContractKey(contract) // Create a key from the contract

      val innerMap = reqData.getOrElseUpdate(contractKey, TrieMap())
      innerMap.put(TickType.get(tickType).field(), value)
    }
  }

  // unused methods for our use case
  override def tickString(tickerId: Int, tickType: Int, value: String): Unit = {}
  override def tickOptionComputation(tickerId: Int, field: Int, tickAttrib: Int, impliedVol: Double, delta: Double, optPrice: Double, pvDividend: Double, gamma: Double, vega: Double, theta: Double, undPrice: Double): Unit = {}
  override def tickEFP(tickerId: Int, tickType: Int, basisPoints: Double, formattedBasisPoints: String, impliedFuture: Double, holdDays: Int, futureLastTradeDate: String, dividendImpact: Double, dividendsToLastTradeDate: Double): Unit = {}
  override def orderStatus(orderId: Int, status: String, filled: Decimal, remaining: Decimal, avgFillPrice: Double, permId: Long, parentId: Int, lastFillPrice: Double, clientId: Int, whyHeld: String, mktCapPrice: Double): Unit = {}
  override def openOrder(orderId: Int, contract: Contract, order: Order, orderState: OrderState): Unit = {}
  override def openOrderEnd(): Unit = {}
  override def updateAccountValue(key: String, value: String, currency: String, accountName: String): Unit = {}
  override def updatePortfolio(contract: Contract, position: Decimal, marketPrice: Double, marketValue: Double, averageCost: Double, unrealizedPNL: Double, realizedPNL: Double, accountName: String): Unit = {}
  override def updateAccountTime(timeStamp: String): Unit = {}
  override def accountDownloadEnd(accountName: String): Unit = {}
  override def nextValidId(orderId: Int): Unit = {}
  override def bondContractDetails(reqId: Int, contractDetails: ContractDetails): Unit = {}
  override def contractDetailsEnd(reqId: Int): Unit = {}
  override def execDetails(reqId: Int, contract: Contract, execution: Execution): Unit = {}
  override def execDetailsEnd(reqId: Int): Unit = {}
  override def updateMktDepth(tickerId: Int, position: Int, operation: Int, side: Int, price: Double, size: Decimal): Unit = {}
  override def updateMktDepthL2(tickerId: Int, position: Int, marketMaker: String, operation: Int, side: Int, price: Double, size: Decimal, isSmartDepth: Boolean): Unit = {}
  override def updateNewsBulletin(msgId: Int, msgType: Int, message: String, origExchange: String): Unit = {}
  override def managedAccounts(accountsList: String): Unit = {}
  override def receiveFA(faDataType: Int, xml: String): Unit = {}
  override def scannerParameters(xml: String): Unit = {}
  override def scannerData(reqId: Int, rank: Int, contractDetails: ContractDetails, distance: String, benchmark: String, projection: String, legsStr: String): Unit = {}
  override def scannerDataEnd(reqId: Int): Unit = {}
  override def realtimeBar(reqId: Int, time: Long, open: Double, high: Double, low: Double, close: Double, volume: Decimal, wap: Decimal, count: Int): Unit = {}
  override def currentTime(time: Long): Unit = {}
  override def fundamentalData(reqId: Int, data: String): Unit = {}
  override def deltaNeutralValidation(reqId: Int, deltaNeutralContract: DeltaNeutralContract): Unit = {}
  override def tickSnapshotEnd(reqId: Int): Unit = {}
  override def marketDataType(reqId: Int, marketDataType: Int): Unit = {}
  override def commissionAndFeesReport(commissionAndFeesReport: CommissionAndFeesReport): Unit = {}
  override def position(account: String, contract: Contract, pos: Decimal, avgCost: Double): Unit = {}
  override def positionEnd(): Unit = {}
  override def accountSummary(reqId: Int, account: String, tag: String, value: String, currency: String): Unit = {}
  override def accountSummaryEnd(reqId: Int): Unit = {}
  override def verifyMessageAPI(apiData: String): Unit = {}
  override def verifyCompleted(isSuccessful: Boolean, errorText: String): Unit = {}
  override def verifyAndAuthMessageAPI(apiData: String, xyzChallenge: String): Unit = {}
  override def verifyAndAuthCompleted(isSuccessful: Boolean, errorText: String): Unit = {}
  override def displayGroupList(reqId: Int, groups: String): Unit = {}
  override def displayGroupUpdated(reqId: Int, contractInfo: String): Unit = {}
  override def error(str: String): Unit = {}
  override def connectionClosed(): Unit = {}
  override def connectAck(): Unit = {}
  override def positionMulti(reqId: Int, account: String, modelCode: String, contract: Contract, pos: Decimal, avgCost: Double): Unit = {}
  override def positionMultiEnd(reqId: Int): Unit = {}
  override def accountUpdateMulti(reqId: Int, account: String, modelCode: String, key: String, value: String, currency: String): Unit = {}
  override def accountUpdateMultiEnd(reqId: Int): Unit = {}
  override def securityDefinitionOptionalParameter(reqId: Int, exchange: String, underlyingConId: Int, tradingClass: String, multiplier: String, expirations: java.util.Set[String], strikes: java.util.Set[java.lang.Double]): Unit = {}
  override def securityDefinitionOptionalParameterEnd(reqId: Int): Unit = {}
  override def softDollarTiers(reqId: Int, tiers: Array[SoftDollarTier]): Unit = {}
  override def familyCodes(familyCodes: Array[FamilyCode]): Unit = {}
  override def symbolSamples(reqId: Int, contractDescriptions: Array[ContractDescription]): Unit = {}
  override def mktDepthExchanges(depthMktDataDescriptions: Array[DepthMktDataDescription]): Unit = {}
  override def tickNews(tickerId: Int, timeStamp: Long, providerCode: String, articleId: String, headline: String, extraData: String): Unit = {}
  override def smartComponents(reqId: Int, theMap: java.util.Map[java.lang.Integer, java.util.Map.Entry[String, java.lang.Character]]): Unit = {}
  override def tickReqParams(tickerId: Int, minTick: Double, bboExchange: String, snapshotPermissions: Int): Unit = {}
  override def newsProviders(newsProviders: Array[NewsProvider]): Unit = {}
  override def newsArticle(requestId: Int, articleType: Int, articleText: String): Unit = {}
  override def historicalNews(requestId: Int, time: String, providerCode: String, articleId: String, headline: String): Unit = {}
  override def historicalNewsEnd(requestId: Int, hasMore: Boolean): Unit = {}
  override def headTimestamp(reqId: Int, headTimestamp: String): Unit = {}
  override def histogramData(reqId: Int, items: java.util.List[HistogramEntry]): Unit = {}
  override def historicalDataUpdate(reqId: Int, bar: Bar): Unit = {}
  override def rerouteMktDataReq(reqId: Int, conId: Int, exchange: String): Unit = {}
  override def rerouteMktDepthReq(reqId: Int, conId: Int, exchange: String): Unit = {}
  override def marketRule(marketRuleId: Int, priceIncrements: Array[PriceIncrement]): Unit = {}
  override def pnl(reqId: Int, dailyPnL: Double, unrealizedPnL: Double, realizedPnL: Double): Unit = {}
  override def pnlSingle(reqId: Int, pos: Decimal, dailyPnL: Double, unrealizedPnL: Double, realizedPnL: Double, value: Double): Unit = {}
  override def historicalTicks(reqId: Int, ticks: java.util.List[HistoricalTick], done: Boolean): Unit = {}
  override def historicalTicksBidAsk(reqId: Int, ticks: java.util.List[HistoricalTickBidAsk], done: Boolean): Unit = {}
  override def historicalTicksLast(reqId: Int, ticks: java.util.List[HistoricalTickLast], done: Boolean): Unit = {}
  override def tickByTickAllLast(reqId: Int, tickType: Int, time: Long, price: Double, size: Decimal, tickAttribLast: TickAttribLast, exchange: String, specialConditions: String): Unit = {}
  override def tickByTickBidAsk(reqId: Int, time: Long, bidPrice: Double, askPrice: Double, bidSize: Decimal, askSize: Decimal, tickAttribBidAsk: TickAttribBidAsk): Unit = {}
  override def tickByTickMidPoint(reqId: Int, time: Long, midPoint: Double): Unit = {}
  override def orderBound(permId: Long, clientId: Int, orderId: Int): Unit = {}
  override def completedOrder(contract: Contract, order: Order, orderState: OrderState): Unit = {}
  override def completedOrdersEnd(): Unit = {}
  override def replaceFAEnd(reqId: Int, text: String): Unit = {}
  override def wshMetaData(reqId: Int, dataJson: String): Unit = {}
  override def wshEventData(reqId: Int, dataJson: String): Unit = {}
  override def historicalSchedule(reqId: Int, startDateTime: String, endDateTime: String, timeZone: String, sessions: java.util.List[HistoricalSession]): Unit = {}
  override def userInfo(reqId: Int, whiteBrandingId: String): Unit = {}
  override def currentTimeInMillis(timeInMillis: Long): Unit = {}
}