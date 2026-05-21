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
import fr.acinq.bitcoin.scalacompat.{BlockHash, Script, addressToPublicKeyScript}
import scodec.bits.ByteVector

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
 * Two-way index between (peer nodeId) and (swap-in publicKeyScript) so we can:
 *   - on receiving an L1 NewTransaction, look up which peer owns the matching output's script
 *   - on un-register or token loss, remove a peer's entire entry set
 *
 * Pre-computes publicKeyScript bytes once at registration time so the per-tx scanning hot path
 * stays a single map lookup.
 */
class SwapInAddressRegistry(chainHash: BlockHash) {

  /** nodeId → set of publicKeyScripts (ByteVector) */
  private val byPeer = new ConcurrentHashMap[PublicKey, Set[ByteVector]]()

  /** publicKeyScript → nodeId (reverse index for fast hot-path lookup) */
  private val byScript = new ConcurrentHashMap[ByteVector, PublicKey]()

  /**
   * Replace this peer's registered swap-in addresses with a new set. Old entries for the same
   * nodeId are removed from both indices.
   */
  def register(nodeId: PublicKey, addresses: List[String]): Unit = {
    val newScripts: Set[ByteVector] = addresses.flatMap { addr =>
      addressToPublicKeyScript(chainHash, addr) match {
        case Right(scriptElts) => Some(Script.write(scriptElts))
        case Left(_) => None
      }
    }.toSet
    // Remove old reverse-index entries for this peer.
    val oldScripts = Option(byPeer.get(nodeId)).getOrElse(Set.empty)
    (oldScripts diff newScripts).foreach(s => byScript.remove(s, nodeId))
    // Install new entries.
    byPeer.put(nodeId, newScripts)
    newScripts.foreach(s => byScript.put(s, nodeId))
  }

  /** Remove all entries for this peer (used on UnsetFCMToken). */
  def remove(nodeId: PublicKey): Unit = {
    Option(byPeer.remove(nodeId)).foreach(_.foreach(s => byScript.remove(s, nodeId)))
  }

  /** Hot path: given an output's publicKeyScript, return the peer that owns it (if any). */
  def lookup(publicKeyScript: ByteVector): Option[PublicKey] = Option(byScript.get(publicKeyScript))

  def size: Int = byScript.size()
  def peerCount: Int = byPeer.size()
  def snapshot: Map[PublicKey, Set[ByteVector]] = byPeer.asScala.toMap
}
