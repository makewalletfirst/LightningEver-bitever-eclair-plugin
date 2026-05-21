/*
 * Copyright 2026 LightningEver
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package fr.acinq.eclair.plugins.fcmpush

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import akka.util.Timeout
import fr.acinq.bitcoin.scalacompat.{ByteVector32, Crypto, Transaction, TxId}
import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey
import fr.acinq.eclair.blockchain.NewTransaction
import fr.acinq.eclair.channel.Register
import fr.acinq.eclair.io.{FcmTokenRegistered, FcmTokenUnregistered, SwapInAddressesRegistered, WakeUpPeerRequested}
import fr.acinq.eclair.payment.PaymentReceived

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.duration._
import scala.util.{Failure, Success}

object FcmPushActor {
  def props(config: FcmPushConfig, registry: FcmTokenRegistry, swapInRegistry: SwapInAddressRegistry, sender: Option[FcmSender], register: ActorRef): Props =
    Props(new FcmPushActor(config, registry, swapInRegistry, sender, register))
}

/**
 * Subscribes to the eclair EventStream:
 *  - FcmTokenRegistered / FcmTokenUnregistered: maintain the in-memory peer → token map
 *  - SwapInAddressesRegistered: update the swap-in address registry
 *  - WakeUpPeerRequested: BOLT12 / NodeRelay wake-up trigger
 *  - PaymentReceived: post-payment notification
 *  - NewTransaction: scan tx outputs against the swap-in registry to wake offline wallets on L1 deposits
 *
 * Push sends are best-effort and synchronous-blocking (FCM v1 is fast — single-digit ms typically).
 */
class FcmPushActor(
  config: FcmPushConfig,
  registry: FcmTokenRegistry,
  swapInRegistry: SwapInAddressRegistry,
  fcmSender: Option[FcmSender],
  register: ActorRef,
) extends Actor with ActorLogging {

  import context.dispatcher

  implicit val askTimeout: Timeout = Timeout(3.seconds)

  /**
   * Dedup: do not push twice for the same swap-in tx (across multiple ZMQ deliveries of the same
   * mempool entry, or mempool + block confirmation events). Cap entries to avoid unbounded growth.
   */
  private val recentSwapInPushes = new ConcurrentHashMap[TxId, Long]()
  private val SwapInPushTtlMs = 30 * 60 * 1000L // 30 min — covers reorg / re-broadcast window

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, classOf[FcmTokenRegistered])
    context.system.eventStream.subscribe(self, classOf[FcmTokenUnregistered])
    // [LightningEver 2026-05-22] swap-in offline 자동화는 BOLT12 offline 결제에서 force-close
    // 재발 가능성이 확인되어 일시 비활성. SwapInAddressesRegistered / NewTransaction subscribe 차단.
    // 코드는 남겨두고 향후 root-cause 정리 후 재활성 예정.
    // context.system.eventStream.subscribe(self, classOf[SwapInAddressesRegistered])
    context.system.eventStream.subscribe(self, classOf[PaymentReceived])
    context.system.eventStream.subscribe(self, classOf[WakeUpPeerRequested])
    // context.system.eventStream.subscribe(self, classOf[NewTransaction])
    log.info("fcm-push subscribed to EventStream (enabled={}, sender={}, swap-in-auto=DISABLED)", config.enabled, fcmSender.isDefined)
  }

  override def receive: Receive = {

    case FcmTokenRegistered(nodeId, token, platform) =>
      registry.put(nodeId, token)
      log.info("fcm-push token registered: nodeId={} platform={} ...token={} registrySize={}",
        nodeId, platform, token.takeRight(8), registry.size)

    case FcmTokenUnregistered(nodeId) =>
      registry.remove(nodeId)
      swapInRegistry.remove(nodeId)
      log.info("fcm-push token removed: nodeId={} registrySize={} swapInPeers={}",
        nodeId, registry.size, swapInRegistry.peerCount)

    case SwapInAddressesRegistered(nodeId, addresses) =>
      swapInRegistry.register(nodeId, addresses)
      log.info("fcm-push swap-in registered: nodeId={} count={} swapInScripts={} swapInPeers={}",
        nodeId, addresses.size, swapInRegistry.size, swapInRegistry.peerCount)

    case WakeUpPeerRequested(nodeId, reason) =>
      registry.get(nodeId) match {
        case Some(token) =>
          log.info("fcm-push: wake-up requested for nodeId={} reason={} — sending push", nodeId, reason)
          self ! SendPushFor(nodeId, token, reason, Map("node_id_hash" -> nodeIdHash(nodeId)))
        case None =>
          log.debug("fcm-push: wake-up requested for nodeId={} but no token in registry; skipping", nodeId)
      }

    case pr: PaymentReceived =>
      val channelId = pr.parts.head.fromChannelId
      val amount = pr.amount.toLong
      lookupPeerByChannel(channelId).onComplete {
        case Success(Some(nodeId)) =>
          registry.get(nodeId) match {
            case Some(token) =>
              self ! SendPushFor(nodeId, token, "IncomingPayment", Map(
                "amount_msat" -> amount.toString,
                "payment_hash" -> pr.paymentHash.toHex,
                "node_id_hash" -> nodeIdHash(nodeId),
              ))
            case None =>
              log.debug("PaymentReceived for nodeId={} but no FCM token registered; skip push", nodeId)
          }
        case Success(None) =>
          log.warning("PaymentReceived but channelId={} not found in Register; cannot look up peer", channelId)
        case Failure(ex) =>
          log.error(ex, "fcm-push: failed to resolve nodeId for channelId={}", channelId)
      }

    case NewTransaction(tx: Transaction) =>
      if (swapInRegistry.size > 0) handleNewTransaction(tx)

    case SendPushFor(nodeId, token, reason, extra) =>
      fcmSender match {
        case Some(s) =>
          s.sendPush(token, reason, extra) match {
            case FcmSendResult.InvalidToken =>
              log.info("dropping invalid FCM token for nodeId={}", nodeId)
              registry.remove(nodeId)
            case _ => // logged inside sender
          }
        case None =>
          log.debug("fcm-push disabled — skipping send (nodeId={} reason={})", nodeId, reason)
      }
  }

  /**
   * Scan a mempool / block tx for outputs matching any registered swap-in script. If a match is
   * found, push a wake-up to the owning peer (deduplicated per txid).
   */
  private def handleNewTransaction(tx: Transaction): Unit = {
    val txid = tx.txid
    if (recentSwapInPushes.containsKey(txid)) return
    var matched = false
    tx.txOut.foreach { out =>
      if (!matched) {
        swapInRegistry.lookup(out.publicKeyScript) match {
          case Some(nodeId) =>
            matched = true
            registry.get(nodeId) match {
              case Some(token) =>
                log.info("fcm-push: swap-in deposit detected for nodeId={} txid={} amount={} sat — sending push", nodeId, txid, out.amount.toLong)
                recentSwapInPushes.put(txid, System.currentTimeMillis())
                pruneRecent()
                self ! SendPushFor(nodeId, token, "SwapInDeposit", Map(
                  "tx_id" -> txid.value.toHex,
                  "amount_sat" -> out.amount.toLong.toString,
                  "node_id_hash" -> nodeIdHash(nodeId),
                ))
              case None =>
                log.info("fcm-push: swap-in deposit detected for nodeId={} txid={} but no FCM token registered", nodeId, txid)
                recentSwapInPushes.put(txid, System.currentTimeMillis())
                pruneRecent()
            }
          case None => // not one of ours
        }
      }
    }
  }

  /** Best-effort eviction of stale dedup entries — runs cheaply on every match. */
  private def pruneRecent(): Unit = {
    val cutoff = System.currentTimeMillis() - SwapInPushTtlMs
    val it = recentSwapInPushes.entrySet().iterator()
    while (it.hasNext) {
      val e = it.next()
      if (e.getValue < cutoff) it.remove()
    }
  }

  private def lookupPeerByChannel(channelId: ByteVector32): scala.concurrent.Future[Option[PublicKey]] = {
    (register ? Register.GetChannelsTo).mapTo[Map[ByteVector32, PublicKey]].map(_.get(channelId))
  }

  /** Phoenix uses hash160(nodeId) as the wallet/node identifier in FCM payloads. */
  private def nodeIdHash(nodeId: PublicKey): String = Crypto.hash160(nodeId.value).toHex

  private case class SendPushFor(nodeId: PublicKey, token: String, reason: String, extra: Map[String, String])
}
