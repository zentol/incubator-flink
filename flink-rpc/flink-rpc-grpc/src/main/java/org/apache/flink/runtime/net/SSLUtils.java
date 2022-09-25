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

package org.apache.flink.runtime.net;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.util.StringUtils;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.OpenSsl;
import io.netty.handler.ssl.OpenSslX509KeyManagerFactory;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import io.netty.handler.ssl.util.FingerprintTrustManagerFactory;

import javax.annotation.Nullable;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.List;

import static io.netty.handler.ssl.SslProvider.JDK;
import static io.netty.handler.ssl.SslProvider.OPENSSL;
import static io.netty.handler.ssl.SslProvider.OPENSSL_REFCNT;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Common utilities to manage SSL transport settings.
 *
 * <p>This is a partial copy from the flink-runtime SSLUtils. They cannot be shared because they
 * work against different versions of netty (plain netty vs flink-shaded-netty).
 */
public class SSLUtils {

    private static String[] getEnabledProtocols(final Configuration config) {
        checkNotNull(config, "config must not be null");
        return config.getString(SecurityOptions.SSL_PROTOCOL).split(",");
    }

    private static String[] getEnabledCipherSuites(final Configuration config) {
        checkNotNull(config, "config must not be null");
        return config.getString(SecurityOptions.SSL_ALGORITHMS).split(",");
    }

    @VisibleForTesting
    static SslProvider getSSLProvider(final Configuration config) {
        checkNotNull(config, "config must not be null");
        String providerString = config.getString(SecurityOptions.SSL_PROVIDER);
        if (providerString.equalsIgnoreCase("OPENSSL")) {
            if (OpenSsl.isAvailable()) {
                return OPENSSL;
            } else {
                throw new IllegalConfigurationException(
                        "openSSL not available", OpenSsl.unavailabilityCause());
            }
        } else if (providerString.equalsIgnoreCase("JDK")) {
            return JDK;
        } else {
            throw new IllegalConfigurationException("Unknown SSL provider: %s", providerString);
        }
    }

    private static TrustManagerFactory getTrustManagerFactory(
            Configuration config, boolean internal)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        String trustStoreFilePath =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_TRUSTSTORE
                                : SecurityOptions.SSL_REST_TRUSTSTORE,
                        SecurityOptions.SSL_TRUSTSTORE);

        String trustStorePassword =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_TRUSTSTORE_PASSWORD
                                : SecurityOptions.SSL_REST_TRUSTSTORE_PASSWORD,
                        SecurityOptions.SSL_TRUSTSTORE_PASSWORD);

        KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream trustStoreFile =
                Files.newInputStream(new File(trustStoreFilePath).toPath())) {
            trustStore.load(trustStoreFile, trustStorePassword.toCharArray());
        }

        String certFingerprint =
                config.getString(
                        internal
                                ? SecurityOptions.SSL_INTERNAL_CERT_FINGERPRINT
                                : SecurityOptions.SSL_REST_CERT_FINGERPRINT);

        TrustManagerFactory tmf;
        if (StringUtils.isNullOrWhitespaceOnly(certFingerprint)) {
            tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        } else {
            tmf = new FingerprintTrustManagerFactory(certFingerprint.split(","));
        }

        tmf.init(trustStore);

        return tmf;
    }

    private static KeyManagerFactory getKeyManagerFactory(
            Configuration config, boolean internal, SslProvider provider)
            throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
                    UnrecoverableKeyException {
        String keystoreFilePath =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_KEYSTORE
                                : SecurityOptions.SSL_REST_KEYSTORE,
                        SecurityOptions.SSL_KEYSTORE);

        String keystorePassword =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_KEYSTORE_PASSWORD
                                : SecurityOptions.SSL_REST_KEYSTORE_PASSWORD,
                        SecurityOptions.SSL_KEYSTORE_PASSWORD);

        String certPassword =
                getAndCheckOption(
                        config,
                        internal
                                ? SecurityOptions.SSL_INTERNAL_KEY_PASSWORD
                                : SecurityOptions.SSL_REST_KEY_PASSWORD,
                        SecurityOptions.SSL_KEY_PASSWORD);

        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        try (InputStream keyStoreFile = Files.newInputStream(new File(keystoreFilePath).toPath())) {
            keyStore.load(keyStoreFile, keystorePassword.toCharArray());
        }

        final KeyManagerFactory kmf;
        if (provider == OPENSSL || provider == OPENSSL_REFCNT) {
            kmf = new OpenSslX509KeyManagerFactory();
        } else {
            kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        }
        kmf.init(keyStore, certPassword.toCharArray());

        return kmf;
    }

    @Nullable
    public static SslContextBuilder createInternalNettySSLContext(
            Configuration config, boolean clientMode) throws Exception {
        return createInternalNettySSLContext(config, clientMode, getSSLProvider(config));
    }

    /**
     * Creates the SSL Context for internal SSL, if internal SSL is configured. For internal SSL,
     * the client and server side configuration are identical, because of mutual authentication.
     */
    @Nullable
    private static SslContextBuilder createInternalNettySSLContext(
            Configuration config, boolean clientMode, SslProvider provider) throws Exception {
        checkNotNull(config, "config");

        if (!SecurityOptions.isInternalSSLEnabled(config)) {
            return null;
        }

        String[] sslProtocols = getEnabledProtocols(config);
        List<String> ciphers = Arrays.asList(getEnabledCipherSuites(config));
        int sessionCacheSize = config.getInteger(SecurityOptions.SSL_INTERNAL_SESSION_CACHE_SIZE);
        int sessionTimeoutMs = config.getInteger(SecurityOptions.SSL_INTERNAL_SESSION_TIMEOUT);

        KeyManagerFactory kmf = getKeyManagerFactory(config, true, provider);
        TrustManagerFactory tmf = getTrustManagerFactory(config, true);
        ClientAuth clientAuth = ClientAuth.REQUIRE;

        final SslContextBuilder sslContextBuilder;
        if (clientMode) {
            sslContextBuilder = SslContextBuilder.forClient().keyManager(kmf);
        } else {
            sslContextBuilder = SslContextBuilder.forServer(kmf);
        }

        return sslContextBuilder
                .sslProvider(provider)
                .protocols(sslProtocols)
                .ciphers(ciphers)
                .trustManager(tmf)
                .clientAuth(clientAuth)
                .sessionCacheSize(sessionCacheSize)
                .sessionTimeout(sessionTimeoutMs / 1000);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static String getAndCheckOption(
            Configuration config,
            ConfigOption<String> primaryOption,
            ConfigOption<String> fallbackOption) {
        String value = config.getString(primaryOption, config.getString(fallbackOption));
        if (value != null) {
            return value;
        } else {
            throw new IllegalConfigurationException(
                    "The config option "
                            + primaryOption.key()
                            + " or "
                            + fallbackOption.key()
                            + " is missing.");
        }
    }
}
