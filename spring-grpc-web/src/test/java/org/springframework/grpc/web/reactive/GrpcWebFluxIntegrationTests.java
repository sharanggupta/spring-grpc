/*
 * Copyright 2026-present the original author or authors.
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

package org.springframework.grpc.web.reactive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.http.server.reactive.HttpHandler;
import org.springframework.http.server.reactive.ReactorHttpHandlerAdapter;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.server.HandlerStrategies;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import com.example.hello.HelloReply;
import com.example.hello.HelloRequest;
import com.example.hello.SimpleGrpc;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.netty.DisposableServer;
import reactor.netty.http.HttpProtocol;
import reactor.netty.http.server.HttpServer;

/**
 * Integration tests that serve gRPC methods from WebFlux functional routes on Reactor
 * Netty (HTTP/2 cleartext) and call them with a grpc-java client.
 */
class GrpcWebFluxIntegrationTests {

	private final GreetingService service = new GreetingService();

	private DisposableServer server;

	private ManagedChannel channel;

	@BeforeEach
	void startServer() {
		HttpHandler httpHandler = RouterFunctions.toHttpHandler(routes(),
				HandlerStrategies.builder().codecs((codecs) -> {
					codecs.customCodecs().register(new GrpcDecoder());
					codecs.customCodecs().register(new GrpcHttpMessageWriter());
				}).build());
		this.server = HttpServer.create()
			.protocol(HttpProtocol.HTTP11, HttpProtocol.H2C)
			.port(0)
			.handle(new ReactorHttpHandlerAdapter(httpHandler))
			.bindNow();
		this.channel = ManagedChannelBuilder.forAddress("localhost", this.server.port()).usePlaintext().build();
	}

	@AfterEach
	void stopServer() throws InterruptedException {
		this.channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
		this.server.disposeNow();
	}

	private RouterFunction<ServerResponse> routes() {
		return RouterFunctions.route()
			.POST("/Simple/SayHello",
					(request) -> ServerResponse.ok()
						.contentType(GrpcMediaTypes.APPLICATION_GRPC)
						.body(request.bodyToMono(HelloRequest.class)
							.flatMapMany((hello) -> call((replies) -> this.service.sayHello(hello, replies))),
								HelloReply.class))
			.POST("/Simple/StreamHello",
					(request) -> ServerResponse.ok()
						.contentType(GrpcMediaTypes.APPLICATION_GRPC)
						.body(request.bodyToMono(HelloRequest.class)
							.flatMapMany((hello) -> GrpcWebFluxIntegrationTests
								.<HelloReply>call((replies) -> this.service.streamHello(hello, replies)))
							// Reply from another thread than the event loop, as a
							// blocking
							// service would
							.subscribeOn(Schedulers.boundedElastic()), HelloReply.class))
			.POST("/Simple/ParallelHello",
					(request) -> ServerResponse.ok()
						.contentType(GrpcMediaTypes.APPLICATION_GRPC)
						.body(request.bodyToFlux(HelloRequest.class).map(GreetingService::greet), HelloReply.class))
			.GET("/hello", (request) -> ServerResponse.ok().bodyValue("Hello from WebFlux"))
			.build();
	}

	@Test
	void unaryCallReturnsResponseAndOkStatus() {
		ClosingStatus closingStatus = new ClosingStatus();

		HelloReply reply = stub().withInterceptors(closingStatus).sayHello(hello("Alien"));

		assertThat(reply.getMessage()).isEqualTo("Hello Alien");
		assertThat(closingStatus.get().getCode()).isEqualTo(Status.Code.OK);
	}

	@Test
	void serverStreamingCallReturnsEveryMessageAndOkStatus() {
		ClosingStatus closingStatus = new ClosingStatus();
		List<String> messages = new ArrayList<>();

		stub().withInterceptors(closingStatus)
			.streamHello(hello("Alien"))
			.forEachRemaining((reply) -> messages.add(reply.getMessage()));

		assertThat(messages).containsExactly("Hello(0) Alien", "Hello(1) Alien", "Hello(2) Alien");
		assertThat(closingStatus.get().getCode()).isEqualTo(Status.Code.OK);
	}

	@Test
	void serverStreamingCallSendsErrorStatusAfterSomeMessages() {
		List<String> messages = new ArrayList<>();

		assertThatExceptionOfType(StatusRuntimeException.class).isThrownBy(
				() -> stub().streamHello(hello("error")).forEachRemaining((reply) -> messages.add(reply.getMessage())))
			.satisfies((ex) -> {
				assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.ABORTED);
				assertThat(ex.getStatus().getDescription()).isEqualTo("Stream interrupted");
			});
		assertThat(messages).containsExactly("Hello(0) error", "Hello(1) error");
	}

	@Test
	void bidiStreamingCallRepliesToEachMessageBeforeTheClientHalfCloses() throws Exception {
		BlockingQueue<HelloReply> replies = new LinkedBlockingQueue<>();
		CompletableFuture<Status> closed = new CompletableFuture<>();
		StreamObserver<HelloRequest> requests = SimpleGrpc.newStub(this.channel)
			.withDeadlineAfter(10, TimeUnit.SECONDS)
			.parallelHello(new StreamObserver<>() {

				@Override
				public void onNext(HelloReply reply) {
					replies.add(reply);
				}

				@Override
				public void onError(Throwable ex) {
					closed.complete(Status.fromThrowable(ex));
				}

				@Override
				public void onCompleted() {
					closed.complete(Status.OK);
				}

			});

		requests.onNext(hello("Alien"));
		assertThat(replies.poll(5, TimeUnit.SECONDS)).extracting(HelloReply::getMessage).isEqualTo("Hello Alien");
		requests.onNext(hello("Predator"));
		assertThat(replies.poll(5, TimeUnit.SECONDS)).extracting(HelloReply::getMessage).isEqualTo("Hello Predator");
		requests.onCompleted();

		assertThat(closed.get(5, TimeUnit.SECONDS).getCode()).isEqualTo(Status.Code.OK);
		assertThat(replies).isEmpty();
	}

	@Test
	void errorFromServiceIsSentAsGrpcStatusAndMessage() {
		assertThatExceptionOfType(StatusRuntimeException.class)
			.isThrownBy(() -> stub().sayHello(hello("error überall 100%")))
			.satisfies((ex) -> {
				assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
				assertThat(ex.getStatus().getDescription()).isEqualTo("Bad name: error überall 100%");
			});
	}

	@Test
	void compressedRequestIsRejected() {
		assertThatExceptionOfType(StatusRuntimeException.class)
			.isThrownBy(() -> stub().withCompression("gzip").sayHello(hello("Alien")))
			.satisfies((ex) -> {
				assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.INTERNAL);
				assertThat(ex.getStatus().getDescription()).contains("ompress");
			});
	}

	@Test
	void plainWebFluxEndpointSharesThePort() {
		String body = WebClient.create("http://localhost:" + this.server.port())
			.get()
			.uri("/hello")
			.retrieve()
			.bodyToMono(String.class)
			.block();

		assertThat(body).isEqualTo("Hello from WebFlux");
	}

	private SimpleGrpc.SimpleBlockingStub stub() {
		return SimpleGrpc.newBlockingStub(this.channel).withDeadlineAfter(10, TimeUnit.SECONDS);
	}

	private static HelloRequest hello(String name) {
		return HelloRequest.newBuilder().setName(name).build();
	}

	/**
	 * Adapt a grpc-java service method to a {@link Flux} of its responses.
	 */
	private static <T> Flux<T> call(Consumer<StreamObserver<T>> method) {
		return Flux.create((sink) -> method.accept(new StreamObserver<>() {

			@Override
			public void onNext(T value) {
				sink.next(value);
			}

			@Override
			public void onError(Throwable ex) {
				sink.error(ex);
			}

			@Override
			public void onCompleted() {
				sink.complete();
			}

		}));
	}

	static class GreetingService extends SimpleGrpc.SimpleImplBase {

		@Override
		public void sayHello(HelloRequest request, StreamObserver<HelloReply> replies) {
			if (request.getName().startsWith("error")) {
				replies.onError(
						Status.INVALID_ARGUMENT.withDescription("Bad name: " + request.getName()).asRuntimeException());
				return;
			}
			replies.onNext(greet(request));
			replies.onCompleted();
		}

		@Override
		public void streamHello(HelloRequest request, StreamObserver<HelloReply> replies) {
			boolean fail = request.getName().startsWith("error");
			for (int count = 0; count < (fail ? 2 : 3); count++) {
				replies.onNext(HelloReply.newBuilder().setMessage("Hello(" + count + ") " + request.getName()).build());
			}
			if (fail) {
				replies.onError(Status.ABORTED.withDescription("Stream interrupted").asRuntimeException());
				return;
			}
			replies.onCompleted();
		}

		static HelloReply greet(HelloRequest request) {
			return HelloReply.newBuilder().setMessage("Hello " + request.getName()).build();
		}

	}

	/**
	 * Client interceptor that records the status a call was closed with.
	 */
	static class ClosingStatus implements ClientInterceptor {

		private final AtomicReference<Status> status = new AtomicReference<>();

		Status get() {
			return this.status.get();
		}

		@Override
		public <Q, R> ClientCall<Q, R> interceptCall(MethodDescriptor<Q, R> method, CallOptions options, Channel next) {
			return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, options)) {

				@Override
				public void start(Listener<R> listener, Metadata headers) {
					super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {

						@Override
						public void onClose(Status status, Metadata trailers) {
							ClosingStatus.this.status.set(status);
							super.onClose(status, trailers);
						}

					}, headers);
				}

			};
		}

	}

}
