/*
 * Copyright 2024-present the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.grpc.server.scope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.support.ScopeNotActiveException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.grpc.server.GlobalServerInterceptor;

import com.google.protobuf.Empty;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCalls;

/**
 * Tests for {@link GrpcRequestScopeInterceptor}.
 *
 * @author Sharang Gupta
 */
class GrpcRequestScopeInterceptorTests {

	private static final String SERVICE_NAME = "test.ScopedService";

	private static final MethodDescriptor<Empty, Empty> METHOD = MethodDescriptor.<Empty, Empty>newBuilder()
		.setType(MethodDescriptor.MethodType.UNARY)
		.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Get"))
		.setRequestMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
		.setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
		.build();

	@BeforeEach
	void clearDestroyedBeans() {
		CallState.destroyed.clear();
	}

	@Test
	void scopedBeanIsCreatedPerCallAndDestroyedWhenTheCallCompletes() throws Exception {
		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				ScopeConfiguration.class)) {
			GrpcRequestScopeInterceptor scope = context.getBean(GrpcRequestScopeInterceptor.class);
			CallState callState = context.getBean(CallState.class);
			List<String> observedIds = new CopyOnWriteArrayList<>();
			ServerCallHandler<Empty, Empty> handler = (call, headers) -> {
				call.request(1);
				return new ServerCall.Listener<>() {
					@Override
					public void onHalfClose() {
						observedIds.add(callState.getId());
						observedIds.add(callState.getId());
						call.sendHeaders(new Metadata());
						call.sendMessage(Empty.getDefaultInstance());
						call.close(Status.OK, new Metadata());
					}
				};
			};
			ServerServiceDefinition service = ServerServiceDefinition.builder(SERVICE_NAME)
				.addMethod(METHOD, handler)
				.build();
			String serverName = InProcessServerBuilder.generateName();
			Server server = InProcessServerBuilder.forName(serverName)
				.directExecutor()
				.addService(ServerInterceptors.interceptForward(service, scope))
				.build()
				.start();
			ManagedChannel channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
			try {
				CallOptions options = CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS);
				ClientCalls.blockingUnaryCall(channel, METHOD, options, Empty.getDefaultInstance());
				ClientCalls.blockingUnaryCall(channel, METHOD, options, Empty.getDefaultInstance());
			}
			finally {
				channel.shutdownNow();
				server.shutdownNow();
			}
			assertThat(observedIds).hasSize(4);
			assertThat(observedIds.get(1)).isEqualTo(observedIds.get(0));
			assertThat(observedIds.get(3)).isEqualTo(observedIds.get(2)).isNotEqualTo(observedIds.get(0));
			Awaitility.await()
				.atMost(Duration.ofSeconds(5))
				.untilAsserted(() -> assertThat(CallState.destroyed).containsExactlyInAnyOrder(observedIds.get(0),
						observedIds.get(2)));
		}
	}

	@Test
	void scopeIsNotActiveOutsideOfACall() {
		try (AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(
				ScopeConfiguration.class)) {
			CallState callState = context.getBean(CallState.class);
			assertThatThrownBy(callState::getId).isInstanceOf(ScopeNotActiveException.class);
		}
	}

	@Test
	void interceptorRunsBeforeAllOtherInterceptors() {
		assertThat(new GrpcRequestScopeInterceptor().getOrder()).isEqualTo(Ordered.HIGHEST_PRECEDENCE);
	}

	@Configuration(proxyBeanMethods = false)
	static class ScopeConfiguration {

		@Bean
		@GlobalServerInterceptor
		static GrpcRequestScopeInterceptor grpcRequestScopeInterceptor() {
			return new GrpcRequestScopeInterceptor();
		}

		@Bean
		@GrpcRequestScope
		CallState callState() {
			return new CallState();
		}

	}

	static class CallState implements DisposableBean {

		static final List<String> destroyed = new CopyOnWriteArrayList<>();

		private final String id = UUID.randomUUID().toString();

		public String getId() {
			return this.id;
		}

		@Override
		public void destroy() {
			destroyed.add(this.id);
		}

	}

}
