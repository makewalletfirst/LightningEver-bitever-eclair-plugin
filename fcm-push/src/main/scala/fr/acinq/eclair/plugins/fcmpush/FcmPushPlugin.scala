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

import com.typesafe.config.ConfigFactory
import fr.acinq.eclair.{Kit, Plugin, PluginParams, Setup}
import grizzled.slf4j.Logging

import java.io.File

/**
 * LightningEver FCM push plugin.
 *
 *  - Subscribes to EventStream for FcmTokenRegistered / FcmTokenUnregistered (emitted by Peer.scala
 *    when peers send the Phoenix FCMToken / UnsetFCMToken lightning messages — tags 35017 / 35019).
 *  - Maintains an in-process peer_nodeId → device token map.
 *  - On PaymentReceived, resolves the receiving peer via Register and sends an FCM HTTP v1 push
 *    so a backgrounded Phoenix wallet wakes up and pulls the HTLC.
 *
 *  Config precedence:
 *    1. JVM -D system properties
 *    2. <datadir>/fcm_push.conf
 *    3. reference.conf bundled in this jar
 */
class FcmPushPlugin extends Plugin with Logging {

  private var config: FcmPushConfig = _
  private val registry: FcmTokenRegistry = new FcmTokenRegistry
  private var swapInRegistry: SwapInAddressRegistry = _

  override def params: PluginParams = new PluginParams {
    override def name: String = "FcmPushPlugin"
  }

  override def onSetup(setup: Setup): Unit = {
    config = loadConfig(setup.datadir)
    logger.info(s"fcm-push: loaded config enabled=${config.enabled} projectId=${config.projectId} serviceAccount=${config.serviceAccountFile}")
  }

  override def onKit(kit: Kit): Unit = {
    val fcmSender: Option[FcmSender] =
      if (!config.enabled) {
        logger.info("fcm-push: plugin loaded in DISABLED mode (token tracking only, no send)")
        None
      } else {
        try {
          val account = ServiceAccount.load(config.serviceAccountFile)
          if (account.projectId != config.projectId) {
            logger.warn(s"fcm-push: service-account project_id=${account.projectId} does not match config project-id=${config.projectId}")
          }
          val oauth = new FcmOAuth2(account, config.accessTokenRefreshMargin.toSeconds, config.httpTimeout.toMillis.toInt)
          Some(new FcmSender(config, oauth))
        } catch {
          case ex: Throwable =>
            logger.error(s"fcm-push: failed to initialise sender — running in DISABLED mode. cause=${ex.getMessage}", ex)
            None
        }
      }

    swapInRegistry = new SwapInAddressRegistry(kit.nodeParams.chainHash)
    kit.system.actorOf(
      FcmPushActor.props(config, registry, swapInRegistry, fcmSender, kit.register),
      name = "fcm-push-actor",
    )
    logger.info("fcm-push: actor started")
  }

  private def loadConfig(datadir: File): FcmPushConfig = {
    // ConfigFactory.load() defaults to the thread context classloader which, under eclair's
    // plugin loader, is the parent classloader that does not see our jar's reference.conf.
    // Explicitly point at our own classloader so the bundled reference.conf is found.
    val classloader = getClass.getClassLoader
    val merged = ConfigFactory.systemProperties()
      .withFallback(ConfigFactory.parseFile(new File(datadir, "fcm_push.conf")))
      .withFallback(ConfigFactory.load(classloader))
      .resolve()
    FcmPushConfig(merged)
  }
}
