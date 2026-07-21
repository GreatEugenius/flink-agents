/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.agents.runtime.operator.coordinator;

import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Content identity of an AgentPlan: {@code planId = sha256(canonicalPlanJson)}.
 *
 * <p>The coordinator canonicalizes the submitted JSON once and mints the planId; subtasks verify
 * the signature over the received bytes. The planId only answers "is this the same plan" — recency
 * is expressed separately by the latest pointer ({@code planVersion + latestPlanId}).
 */
public final class PlanIds {

    private static final JsonMapper CANONICAL_MAPPER =
            JsonMapper.builder()
                    .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                    .build();

    private PlanIds() {}

    /**
     * Rewrites JSON into canonical form: recursively sorted object keys, no insignificant
     * whitespace. Two semantically identical plans always canonicalize to the same string.
     */
    public static String canonicalize(String json) throws IOException {
        Object tree = CANONICAL_MAPPER.readValue(json, Object.class);
        return CANONICAL_MAPPER.writeValueAsString(tree);
    }

    /** sha256 hex over the exact JSON string bytes (UTF-8). */
    public static String planIdOf(String json) {
        return sha256Hex(json.getBytes(StandardCharsets.UTF_8));
    }

    /** sha256 hex of a local file's contents; used to verify plan artifacts before loading. */
    public static String sha256HexOfFile(Path path) throws IOException {
        return sha256Hex(Files.readAllBytes(path));
    }

    public static String sha256Hex(byte[] bytes) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(bytes);
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable.", e);
        }
    }
}
