/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rpc.akka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.japi.pf.ReceiveBuilder;
import akka.pattern.Patterns;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the {@link akka.actor.ActorSystem} instantiated through {@link
 * org.apache.flink.runtime.rpc.akka.AkkaUtils}.
 */
@Execution(ExecutionMode.CONCURRENT)
class AkkaActorSystemTest {

    @Test
    void shutsDownOnActorFailure() {
        final ActorSystem actorSystem = AkkaUtils.createLocalActorSystem(new Configuration());

        try {
            final CompletableFuture<Terminated> terminationFuture =
                    actorSystem.getWhenTerminated().toCompletableFuture();
            final ActorRef actorRef = actorSystem.actorOf(Props.create(SimpleActor.class));

            final FlinkException cause = new FlinkException("Flink test exception");

            actorRef.tell(Fail.exceptionally(cause), ActorRef.noSender());

            // make sure that the ActorSystem shuts down
            terminationFuture.join();
        } finally {
            AkkaUtils.terminateActorSystem(actorSystem).join();
        }
    }

    @Test
    void askTerminatedActorFailsWithRecipientTerminatedException() {
        final ActorSystem actorSystem = AkkaUtils.createLocalActorSystem(new Configuration());
        final Duration timeout = Duration.ofSeconds(10L);

        try {
            final ActorRef actorRef = actorSystem.actorOf(Props.create(SimpleActor.class));

            // wait for the actor's termination
            Patterns.gracefulStop(actorRef, timeout).toCompletableFuture().join();

            final CompletionStage<Object> result = Patterns.ask(actorRef, new Object(), timeout);

            assertThatThrownBy(() -> result.toCompletableFuture().get())
                    .extracting(ExceptionUtils::stripExecutionException)
                    .matches(AkkaRpcServiceUtils::isRecipientTerminatedException);
        } finally {
            AkkaUtils.terminateActorSystem(actorSystem).join();
        }
    }

    private static final class SimpleActor extends AbstractActor {

        @Override
        public Receive createReceive() {
            return ReceiveBuilder.create().match(Fail.class, this::handleFail).build();
        }

        private void handleFail(Fail fail) {
            throw new RuntimeException(fail.getErrorCause());
        }
    }

    private static final class Fail {
        private final Throwable errorCause;

        private Fail(Throwable errorCause) {
            this.errorCause = errorCause;
        }

        private Throwable getErrorCause() {
            return errorCause;
        }

        private static Fail exceptionally(Throwable errorCause) {
            return new Fail(errorCause);
        }
    }
}
