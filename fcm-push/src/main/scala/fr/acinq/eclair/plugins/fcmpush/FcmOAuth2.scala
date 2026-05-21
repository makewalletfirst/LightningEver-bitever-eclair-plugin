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

import grizzled.slf4j.Logging
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{DefaultFormats, JValue}

import java.net.{HttpURLConnection, URI, URLEncoder}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.security.spec.PKCS8EncodedKeySpec
import java.security.{KeyFactory, Signature}
import java.util.Base64

case class ServiceAccount(
  projectId: String,
  privateKeyId: String,
  privateKeyPem: String,
  clientEmail: String,
  tokenUri: String,
)

object ServiceAccount {
  def load(path: String): ServiceAccount = {
    val text = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
    implicit val formats: DefaultFormats.type = DefaultFormats
    val json: JValue = parse(text)
    ServiceAccount(
      projectId     = (json \ "project_id").extract[String],
      privateKeyId  = (json \ "private_key_id").extract[String],
      privateKeyPem = (json \ "private_key").extract[String],
      clientEmail   = (json \ "client_email").extract[String],
      tokenUri      = (json \ "token_uri").extractOpt[String].getOrElse("https://oauth2.googleapis.com/token"),
    )
  }
}

/**
 * Exchanges a Firebase service-account private key for short-lived OAuth2 access tokens
 * usable against the FCM HTTP v1 API.
 *
 * Implements RFC 7523 (JWT Bearer) entirely with the JDK — no extra Google client lib needed.
 *
 * Thread-safe; refreshes the token in-process when it expires within `refreshMarginSeconds`.
 */
class FcmOAuth2(account: ServiceAccount, refreshMarginSeconds: Long, httpTimeoutMs: Int) extends Logging {

  private val scope = "https://www.googleapis.com/auth/firebase.messaging"
  private val b64Url = Base64.getUrlEncoder.withoutPadding()
  private val privateKey = parsePrivateKey(account.privateKeyPem)

  // synchronized access to cached token + expiry epoch seconds
  private val lock = new Object
  private var cachedToken: String = _
  private var cachedExpiryEpoch: Long = 0L

  def accessToken(): String = lock.synchronized {
    val nowSec = System.currentTimeMillis() / 1000L
    if (cachedToken == null || cachedExpiryEpoch - nowSec < refreshMarginSeconds) {
      refresh(nowSec)
    }
    cachedToken
  }

  private def refresh(nowSec: Long): Unit = {
    val jwt = buildJwt(nowSec)
    val body =
      "grant_type=" + URLEncoder.encode("urn:ietf:params:oauth:grant-type:jwt-bearer", StandardCharsets.UTF_8) +
      "&assertion=" + URLEncoder.encode(jwt, StandardCharsets.UTF_8)

    val conn = URI.create(account.tokenUri).toURL.openConnection().asInstanceOf[HttpURLConnection]
    conn.setRequestMethod("POST")
    conn.setDoOutput(true)
    conn.setConnectTimeout(httpTimeoutMs)
    conn.setReadTimeout(httpTimeoutMs)
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded")

    val os = conn.getOutputStream
    try os.write(body.getBytes(StandardCharsets.UTF_8)) finally os.close()

    val code = conn.getResponseCode
    val stream = if (code >= 200 && code < 300) conn.getInputStream else conn.getErrorStream
    val respText = new String(stream.readAllBytes(), StandardCharsets.UTF_8)
    if (code < 200 || code >= 300) {
      throw new RuntimeException(s"OAuth2 token endpoint returned HTTP $code: $respText")
    }

    implicit val formats: DefaultFormats.type = DefaultFormats
    val json: JValue = parse(respText)
    cachedToken = (json \ "access_token").extract[String]
    val expiresIn = (json \ "expires_in").extract[Long]
    cachedExpiryEpoch = nowSec + expiresIn
    logger.info(s"OAuth2 access token refreshed; expires in ${expiresIn}s")
  }

  private def buildJwt(nowSec: Long): String = {
    val header = s"""{"alg":"RS256","typ":"JWT","kid":"${account.privateKeyId}"}"""
    val claim  =
      s"""{"iss":"${account.clientEmail}","scope":"$scope","aud":"${account.tokenUri}","exp":${nowSec + 3600},"iat":$nowSec}"""

    val headerB64 = b64Url.encodeToString(header.getBytes(StandardCharsets.UTF_8))
    val claimB64  = b64Url.encodeToString(claim.getBytes(StandardCharsets.UTF_8))
    val signingInput = s"$headerB64.$claimB64"

    val signer = Signature.getInstance("SHA256withRSA")
    signer.initSign(privateKey)
    signer.update(signingInput.getBytes(StandardCharsets.UTF_8))
    val sigB64 = b64Url.encodeToString(signer.sign())

    s"$signingInput.$sigB64"
  }

  private def parsePrivateKey(pem: String): java.security.PrivateKey = {
    val cleaned = pem
      .replace("-----BEGIN PRIVATE KEY-----", "")
      .replace("-----END PRIVATE KEY-----", "")
      .replaceAll("\\s+", "")
    val der = Base64.getDecoder.decode(cleaned)
    val spec = new PKCS8EncodedKeySpec(der)
    KeyFactory.getInstance("RSA").generatePrivate(spec)
  }
}
