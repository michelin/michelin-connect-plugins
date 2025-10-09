/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.michelin.kafka.config.providers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.HashSet;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class AES256ConfigProviderTest {
    @Test
    void DecryptFailure_NotBase64() {
        try (final var configProvider = new AES256ConfigProvider()) {
            final var configs = new HashMap<String, String>();
            configs.put("key", "key-aaaabbbbccccdddd");
            configs.put("salt", "salt-aaaabbbbccccdddd");
            configProvider.configure(configs);

            final var wrongKey = new HashSet<String>();
            wrongKey.add("does_not_match"); // secret can't be decoded
            assertThrows(ConfigException.class, () -> configProvider.get("", wrongKey));
        }
    }

    @Test
    void DecryptFailure_InvalidKey() {
        try (final var configProvider = new AES256ConfigProvider()) {
            final var configs = new HashMap<String, String>();
            configs.put("key", "key-aaaabbbbccccdddd");
            configs.put("salt", "salt-aaaabbbbccccdddd");
            configProvider.configure(configs);

            final var wrongKey = new HashSet<String>();
            wrongKey.add("mfw43l96122yZiDhu2RevQ=="); // secret can't be decoded
            assertThrows(ConfigException.class, () -> configProvider.get("", wrongKey));
        }
    }

    @Test
    void DecryptSuccess() {
        final var originalPassword = "hello !";
        final var encodedPassword = "hgkWF2Gp3qPxcPnVifDgJA==";
        try (final var configProvider = new AES256ConfigProvider()) {
            final var configs = new HashMap<String, String>();
            configs.put("key", "key-aaaabbbbccccdddd");
            configs.put("salt", "salt-aaaabbbbccccdddd");
            configProvider.configure(configs);

            // String encoded = AES256Helper.encrypt("aaaabbbbccccdddd",AES256ConfigProvider.DEFAULT_SALT,
            // originalPassword);
            // System.out.println(encoded);

            final var rightKeys = new HashSet<String>();
            rightKeys.add(encodedPassword);

            assertEquals(
                    originalPassword, configProvider.get("", rightKeys).data().get(encodedPassword));
        }
    }

    @Test
    void DecryptSuccessBothAlgorithm() {
        final var originalPassword = "hello !";
        final var encodedPassword = "hgkWF2Gp3qPxcPnVifDgJA==";
        final var encodedPasswordNS4K1 = "TlM0SxO5N1cXueLtuDRVEBCAacMVm/4dwYN0/SMjKGlnkNXFDDt7";
        final var encodedPasswordNS4K2 = "TlM0S9g7l18K6q6pTXs5NGVL2vRVDyHbQ6NDliGPh6UlgL4+6MEX";
        try (final var configProvider = new AES256ConfigProvider()) {
            final var configs = new HashMap<String, String>();
            configs.put("key", "key-aaaabbbbccccdddd");
            configs.put("salt", "salt-aaaabbbbccccdddd");
            configProvider.configure(configs);

            // String encoded = AES256Helper.encrypt("aaaabbbbccccdddd",AES256ConfigProvider.DEFAULT_SALT,
            // originalPassword);
            // System.out.println(encoded);

            final var rightKeys = new HashSet<String>();
            rightKeys.add(encodedPassword);
            rightKeys.add(encodedPasswordNS4K1);
            rightKeys.add(encodedPasswordNS4K2);

            var result = configProvider.get("", rightKeys).data();
            assertEquals(originalPassword, result.get(encodedPassword));
            assertEquals(originalPassword, result.get(encodedPasswordNS4K1));
            assertEquals(originalPassword, result.get(encodedPasswordNS4K2));
        }
    }

    @Test
    void MissingConfig_key() {
        try (final var configProvider = new AES256ConfigProvider()) {
            final var configs = new HashMap<String, String>();
            configs.put("salt", "salt-aaaabbbbccccdddd");

            assertThrows(ConfigException.class, () -> configProvider.configure(configs));
        }
    }

    @Test
    void MissingConfig_salt() {
        try (final var configProvider = new AES256ConfigProvider()) {
            final var configs = new HashMap<String, String>();
            configs.put("key", "key-aaaabbbbccccdddd");

            assertThrows(ConfigException.class, () -> configProvider.configure(configs));
        }
    }
}
