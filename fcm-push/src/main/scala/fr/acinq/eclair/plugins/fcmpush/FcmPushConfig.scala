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

import com.typesafe.config.Config

import scala.concurrent.duration._

case class FcmPushConfig(
  enabled: Boolean,
  serviceAccountFile: String,
  projectId: String,
  fcmEndpoint: String,
  accessTokenRefreshMargin: FiniteDuration,
  httpTimeout: FiniteDuration,
  androidPriority: String,
  androidTtl: FiniteDuration,
)

object FcmPushConfig {
  def apply(config: Config): FcmPushConfig = {
    val c = config.getConfig("fcm-push")
    FcmPushConfig(
      enabled = c.getBoolean("enabled"),
      serviceAccountFile = c.getString("service-account-file"),
      projectId = c.getString("project-id"),
      fcmEndpoint = c.getString("fcm-endpoint"),
      accessTokenRefreshMargin = c.getInt("access-token-refresh-margin-seconds").seconds,
      httpTimeout = c.getInt("http-timeout-seconds").seconds,
      androidPriority = c.getString("android-priority"),
      androidTtl = c.getInt("android-ttl-seconds").seconds,
    )
  }
}
