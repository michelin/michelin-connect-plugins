/*
 * This Java source file was generated by the Gradle 'init' task.
 */
package com.michelin.kafka.config.providers;

import com.michelin.kafka.config.providers.AES256ConfigProvider;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.errors.DataException;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.spec.KeySpec;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class AES256ConfigProviderTest {
    @Test
    void DecryptFailure_NotBase64() {
        AES256ConfigProvider configProvider = new AES256ConfigProvider();

        Map<String, String> configs = new HashMap<>();
        configs.put("key", "key-aaaabbbbccccdddd");
        configs.put("salt", "salt-aaaabbbbccccdddd");
        configProvider.configure(configs);

        Set<String> wrongKey = new HashSet<>();
        wrongKey.add("does_not_match"); // secret can't be decoded
        assertThrows(DataException.class, () -> configProvider.get("", wrongKey));
    }
    @Test
    void DecryptFailure_InvalidKey() {
        AES256ConfigProvider configProvider = new AES256ConfigProvider();

        Map<String, String> configs = new HashMap<>();
        configs.put("key", "key-aaaabbbbccccdddd");
        configs.put("salt", "salt-aaaabbbbccccdddd");
        configProvider.configure(configs);

        Set<String> wrongKey = new HashSet<>();
        wrongKey.add("mfw43l96122yZiDhu2RevQ=="); // secret can't be decoded
        assertThrows(DataException.class, () -> configProvider.get("", wrongKey));
    }
    @Test
    void DecryptSuccess(){
        String originalPassword = "hello !";
        String encodedPassword = "hgkWF2Gp3qPxcPnVifDgJA==";

        AES256ConfigProvider configProvider = new AES256ConfigProvider();

        Map<String, String> configs = new HashMap<>();
        configs.put("key", "key-aaaabbbbccccdddd");
        configs.put("salt", "salt-aaaabbbbccccdddd");
        configProvider.configure(configs);

        // String encoded = AES256Helper.encrypt("aaaabbbbccccdddd",AES256ConfigProvider.DEFAULT_SALT, originalPassword);
        // System.out.println(encoded);

        Set<String> rightKeys = new HashSet<>();
        rightKeys.add(encodedPassword);

        assertEquals(originalPassword, configProvider.get("", rightKeys).data().get(encodedPassword));
    }
    @Test
    void MissingConfig_key(){
        AES256ConfigProvider configProvider = new AES256ConfigProvider();

        Map<String, String> configs = new HashMap<>();
        configs.put("salt", "salt-aaaabbbbccccdddd");

        assertThrows(ConfigException.class, () -> configProvider.configure(configs));
    }
    @Test
    void MissingConfig_salt(){
        AES256ConfigProvider configProvider = new AES256ConfigProvider();

        Map<String, String> configs = new HashMap<>();
        configs.put("key", "key-aaaabbbbccccdddd");

        assertThrows(ConfigException.class, () -> configProvider.configure(configs));
    }
    static class AES256Helper {
        public static String encrypt(String key, String salt, String strToEncrypt) {
            try {
                byte[] iv = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
                IvParameterSpec ivspec = new IvParameterSpec(iv);

                SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
                KeySpec spec = new PBEKeySpec(key.toCharArray(), salt.getBytes(), 65536, 256);
                SecretKey tmp = factory.generateSecret(spec);
                SecretKeySpec secretKey = new SecretKeySpec(tmp.getEncoded(), "AES");

                Cipher cipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
                cipher.init(Cipher.ENCRYPT_MODE, secretKey, ivspec);
                return Base64.getEncoder()
                        .encodeToString(cipher.doFinal(strToEncrypt.getBytes(StandardCharsets.UTF_8)));
            } catch (Exception e) {
                System.out.println("Error while encrypting: " + e.toString());
            }
            return null;
        }
    }
}
