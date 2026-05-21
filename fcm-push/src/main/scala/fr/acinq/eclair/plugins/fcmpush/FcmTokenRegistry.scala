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

import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
 * Process-local registry mapping peer nodeId → latest FCM device token reported by that peer.
 *
 * Phoenix wallets push a fresh token on every reconnect, so we just overwrite. When the peer
 * sends UnsetFCMToken we drop the entry. Restart of the eclair process clears the map — peers
 * will re-register on their next reconnect, so this is intentional (avoids stale tokens).
 */
class FcmTokenRegistry {
  private val tokens = new ConcurrentHashMap[PublicKey, String]()

  def put(nodeId: PublicKey, token: String): Unit = {
    tokens.put(nodeId, token)
  }

  def remove(nodeId: PublicKey): Unit = {
    tokens.remove(nodeId)
  }

  def get(nodeId: PublicKey): Option[String] = Option(tokens.get(nodeId))

  def size: Int = tokens.size()

  def snapshot: Map[PublicKey, String] = tokens.asScala.toMap
}
