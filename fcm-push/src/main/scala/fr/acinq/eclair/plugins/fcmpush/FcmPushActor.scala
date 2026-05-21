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
import fr.acinq.bitcoin.scalacompat.{ByteVector32, Crypto}
import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey
import fr.acinq.eclair.channel.Register
import fr.acinq.eclair.io.{FcmTokenRegistered, FcmTokenUnregistered, WakeUpPeerRequested}
import fr.acinq.eclair.payment.PaymentReceived

import scala.concurrent.duration._
import scala.util.{Failure, Success}

object FcmPushActor {
  def props(config: FcmPushConfig, registry: FcmTokenRegistry, sender: Option[FcmSender], register: ActorRef): Props =
    Props(new FcmPushActor(config, registry, sender, register))
}

/**
 * Subscribes to the eclair EventStream:
 *  - FcmTokenRegistered / FcmTokenUnregistered: maintain the in-memory peer → token map
 *  - PaymentReceived: look up the peer from the first part's channelId, then send an FCM push.
 *
 * Push sends are best-effort and synchronous-blocking (FCM v1 is fast — single-digit ms typically).
 * If we ever need higher throughput we can hand off to a dedicated dispatcher.
 */
class FcmPushActor(
  config: FcmPushConfig,
  registry: FcmTokenRegistry,
  fcmSender: Option[FcmSender],
  register: ActorRef,
) extends Actor with ActorLogging {

  import context.dispatcher

  implicit val askTimeout: Timeout = Timeout(3.seconds)

  override def preStart(): Unit = {
    context.system.eventStream.subscribe(self, classOf[FcmTokenRegistered])
    context.system.eventStream.subscribe(self, classOf[FcmTokenUnregistered])
    context.system.eventStream.subscribe(self, classOf[PaymentReceived])
    context.system.eventStream.subscribe(self, classOf[WakeUpPeerRequested])
    log.info("fcm-push subscribed to EventStream (enabled={}, sender={})", config.enabled, fcmSender.isDefined)
  }

  override def receive: Receive = {

    case FcmTokenRegistered(nodeId, token, platform) =>
      registry.put(nodeId, token)
      log.info("fcm-push token registered: nodeId={} platform={} ...token={} registrySize={}",
        nodeId, platform, token.takeRight(8), registry.size)

    case FcmTokenUnregistered(nodeId) =>
      registry.remove(nodeId)
      log.info("fcm-push token removed: nodeId={} registrySize={}", nodeId, registry.size)

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

  private def lookupPeerByChannel(channelId: ByteVector32): scala.concurrent.Future[Option[PublicKey]] = {
    (register ? Register.GetChannelsTo).mapTo[Map[ByteVector32, PublicKey]].map(_.get(channelId))
  }

  /** Phoenix uses hash160(nodeId) as the wallet/node identifier in FCM payloads. */
  private def nodeIdHash(nodeId: PublicKey): String = Crypto.hash160(nodeId.value).toHex

  private case class SendPushFor(nodeId: PublicKey, token: String, reason: String, extra: Map[String, String])
}
