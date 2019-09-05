/*
 * Copyright 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.training.appdev.backend;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.*;


import com.google.training.appdev.services.gcp.domain.Feedback;
import com.google.training.appdev.services.gcp.languageapi.LanguageService;
import com.google.training.appdev.services.gcp.spanner.SpannerService;

public class ConsoleApp {

  public static void main(String... args) throws Exception {


    String projectId = System.getenv("GCLOUD_PROJECT");
    System.out.println("Project: " + projectId);


    // Notice that the code to create the topic is the same as in the publisher
    TopicName topicName = TopicName.create(projectId, "feedback");


    LanguageService languageService = LanguageService.create();

    SpannerService spannerService = SpannerService.create();

    SubscriptionName subscriptionName = SubscriptionName.create(projectId,
                                                                "worker1-subscription");

    try(SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create()){
        Subscription subscription = subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 0);
    }
    


    // The message receiver processes Pub/Sub subscription messages
    MessageReceiver receiver = getMessageReceiver(languageService, spannerService);
    Subscriber subscriber = null;
    

    // END TODO

    try {

        // TODO: Initialize the subscriber using its default builder
        // with a subscription and receiver
        subscriber = Subscriber.defaultBuilder(subscriptionName, receiver).build();
        subscriber.addListener( new Subscriber.Listener(){
            @Override
            public void failed(ApiService.State from, Throwable failure) {
                super.failed(from, failure);
            }
        },MoreExecutors.directExecutor() );

        subscriber.startAsync().awaitRunning();
        System.out.println("Started. Press any key to quit and remove subscription");
        System.in.read();

    } finally {
        if (subscriber != null) {
            subscriber.stopAsync().awaitTerminated();
        }


        try (SubscriptionAdminClient
                     subscriptionAdminClient =
                     SubscriptionAdminClient.create()) {

            subscriptionAdminClient.deleteSubscription(
                    subscriptionName);
        }

    }
    }

    private static MessageReceiver getMessageReceiver(LanguageService languageService, SpannerService spannerService) {
        return new MessageReceiver() {

            @Override
            public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                // TODO: Extract the message data as a JSON String
                String jsonMessage = message.getData().toStringUtf8();
                consumer.ack();
                try {
                    // Object mapper deserializes the JSON String
                    ObjectMapper mapper = new ObjectMapper();
                    Feedback feedback = mapper.readValue(jsonMessage, Feedback.class);
                    float sentiment = languageService.analyzeSentiment(feedback.getFeedback());
                    feedback.setSentimentScore(sentiment);

                    // TODO: Insert the feedback into Cloud Spanner
                    spannerService.insertFeedback(feedback);


                    // END TODO

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
    }

}

