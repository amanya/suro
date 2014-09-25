package com.netflix.suro.sink.sqs;

/*
 * Copyright 2013 Netflix, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.suro.jackson.DefaultObjectMapper;
import com.netflix.suro.sink.Sink;
import org.junit.Test;

import java.io.*;

import static org.junit.Assert.*;

public class TestSqsSink {
    private static Injector injector = Guice.createInjector(
            new SqsSinkSuroPlugin(),
            new AbstractModule() {
                @Override
                protected void configure() {
                    bind(ObjectMapper.class).to(DefaultObjectMapper.class);
                    bind(AWSCredentialsProvider.class)
                            .toInstance(new AWSCredentialsProvider() {
                                @Override
                                public AWSCredentials getCredentials() {
                                    return new AWSCredentials() {
                                        @Override
                                        public String getAWSAccessKeyId() {
                                            return "accessKey";
                                        }

                                        @Override
                                        public String getAWSSecretKey() {
                                            return "secretKey";
                                        }
                                    };
                                }

                                @Override
                                public void refresh() {

                                }
                            });
                }
            }
    );

    @Test
    public void testDefaultParameters() throws IOException {
        final String sqsSinkSpec = "{\n" +
                "    \"type\": \"" + SqsSink.TYPE + "\",\n" +
                "    \"region\": \"eu-west-1\",\n" +
                "    \"client.id\": \"sqs-test\",\n" +
                "    \"queueName\": \"sqs-queue\"\n" +
                "}";

        ObjectMapper mapper = injector.getInstance(ObjectMapper.class);
        Sink sink = mapper.readValue(sqsSinkSpec, new TypeReference<Sink>(){});
        sink.open();

        assertNull(sink.recvNotice());

        sink.close();

        System.out.println(sink.getStat());
    }
}

