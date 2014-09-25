package com.netflix.suro.sink.sqs;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.GetQueueUrlRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.netflix.suro.message.MessageContainer;
import com.netflix.suro.queue.MemoryQueue4Sink;
import com.netflix.suro.queue.MessageQueue4Sink;
import com.netflix.suro.sink.ThreadPoolQueuedSink;
import com.netflix.suro.sink.kafka.SuroKeyedMessage;
import kafka.producer.KeyedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.suro.message.Message;
import com.netflix.suro.sink.Sink;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * SQS sink implementation using AmazonSQSClient
 * @author amanya
 *
 */
public class SqsSink extends ThreadPoolQueuedSink implements Sink {
    static Logger log = LoggerFactory.getLogger(SqsSink.class);

    public static final String TYPE = "sqs";

    private String clientId;
    private final Map<String, String> keyTopicMap;

    private long msgId = 0;

    private AmazonSQSClient sqsClient;
    private final AWSCredentialsProvider credentialsProvider;
    private String queueUrl;
    private String queueName;
    private final String region;

    private ClientConfiguration clientConfig;

    @JsonCreator
    public SqsSink(
            @JsonProperty("queue4Sink")           MessageQueue4Sink queue4Sink,
            @JsonProperty("client.id")            String clientId,
            @JsonProperty("region")               String region,
            @JsonProperty("queueName")            String queueName,
            @JsonProperty("connectionTimeout")    int connectionTimeout,
            @JsonProperty("maxConnections")       int maxConnections,
            @JsonProperty("socketTimeout")        int socketTimeout,
            @JsonProperty("maxRetries")           int maxRetries,
            @JsonProperty("keyTopicMap")          Map<String, String> keyTopicMap,
            @JsonProperty("batchSize")            int batchSize,
            @JsonProperty("batchTimeout")         int batchTimeout,
            @JsonProperty("jobQueueSize")         int jobQueueSize,
            @JsonProperty("corePoolSize")         int corePoolSize,
            @JsonProperty("maxPoolSize")          int maxPoolSize,
            @JsonProperty("jobTimeout")           long jobTimeout,
            @JacksonInject AWSCredentialsProvider credentialsProvider
    ) throws Exception {
        super(jobQueueSize, corePoolSize, maxPoolSize, jobTimeout,
                SqsSink.class.getSimpleName() + "-" + clientId);

        Preconditions.checkNotNull(region, "region is needed");
        Preconditions.checkNotNull(queueName, "queueName is needed");
        Preconditions.checkNotNull(clientId);

        this.clientId = clientId;
        initialize("sqs_" + clientId, queue4Sink == null ? new MemoryQueue4Sink(10000) : queue4Sink, batchSize, batchTimeout);

        this.keyTopicMap = keyTopicMap != null ? keyTopicMap : Maps.<String, String>newHashMap();


        this.credentialsProvider = credentialsProvider;

        this.region = region;
        this.queueName = queueName;

        clientConfig = new ClientConfiguration();
        if (connectionTimeout > 0) {
            clientConfig = clientConfig.withConnectionTimeout(connectionTimeout);
        }
        if (maxConnections > 0) {
            clientConfig = clientConfig.withMaxConnections(maxConnections);
        }
        if (socketTimeout > 0) {
            clientConfig = clientConfig.withSocketTimeout(socketTimeout);
        }
        if (maxRetries > 0) {
            clientConfig = clientConfig.withMaxErrorRetry(maxRetries);
        }
    }
    
    public void send(List<KeyedMessage<Long, byte[]>> msgList) {
        SendMessageRequest request = new SendMessageRequest()
                .withQueueUrl(queueUrl);

        for (KeyedMessage<Long, byte[]> km : msgList) {
            try {
                String msg = new String(km.message(), "UTF-8");
                request = request.withMessageBody(msg);
                sqsClient.sendMessage(request);
            } catch (UnsupportedEncodingException e) {
                log.error("Error decoding message" + e.getMessage());
            }
        }
    }

    @Override
    public void writeTo(MessageContainer message) {
        long key = msgId++;
        if (!keyTopicMap.isEmpty()) {
            try {
                Map<String, Object> msgMap = message.getEntity(new TypeReference<Map<String, Object>>() {});
                Object keyField = msgMap.get(keyTopicMap.get(message.getRoutingKey()));
                if (keyField != null) {
                    key = keyField.hashCode();
                }
            } catch (Exception e) {
                log.error("Exception on getting key field: " + e.getMessage());
            }
        }

        enqueue(new SuroKeyedMessage(key, message.getMessage()));
    }

    @Override
    public void open() {
        try {
            sqsClient = new AmazonSQSClient(credentialsProvider, clientConfig);

            String endpoint = "sqs." + this.region + ".amazonaws.com";
            sqsClient.setEndpoint(endpoint);

            GetQueueUrlRequest request = new GetQueueUrlRequest();
            request.setQueueName(queueName);

            queueUrl = sqsClient.getQueueUrl(request).getQueueUrl();

            setName(SqsSink.class.getSimpleName() + "-" + clientId);
            start();
        } catch (Exception e) {
            log.error("Failed to start sqs producer");
        }
    }

    @Override
    protected void beforePolling() throws IOException { /*do nothing */}

    @Override
    protected void write(List<Message> msgList) {
        final List<KeyedMessage<Long, byte[]>> sqsMsgList = new ArrayList<KeyedMessage<Long, byte[]>>();
        for (Message m : msgList) {
            SuroKeyedMessage keyedMessage = (SuroKeyedMessage) m;
            sqsMsgList.add(new KeyedMessage<Long, byte[]>(
                    keyedMessage.getRoutingKey(),
                    keyedMessage.getKey(),
                    keyedMessage.getPayload()));
        }

        senders.submit(new Runnable() {
            @Override
            public void run() {
                send(sqsMsgList);
            }
        });
    }

    @Override
    protected void innerClose() {
        super.innerClose();

        close();
    }

    @Override
    public String recvNotice() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getStat() {
        // TODO Auto-generated method stub
        return null;
    }

}
