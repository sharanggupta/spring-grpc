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
import static org.springframework.grpc.web.reactive.GrpcFrames.concat;
import static org.springframework.grpc.web.reactive.GrpcFrames.frame;

import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import org.springframework.core.ResolvableType;
import org.springframework.core.codec.DecodingException;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.MediaType;
import org.springframework.mock.http.server.reactive.MockServerHttpResponse;

import com.example.hello.HelloReply;
import com.google.protobuf.Message;
import io.grpc.Status;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Tests for {@link GrpcHttpMessageWriter}.
 */
class GrpcHttpMessageWriterTests {

	private static final ResolvableType REPLY_TYPE = ResolvableType.forClass(HelloReply.class);

	private static final HelloReply HELLO = HelloReply.newBuilder().setMessage("Hello").build();

	private static final HelloReply GOODBYE = HelloReply.newBuilder().setMessage("Goodbye").build();

	private final GrpcStatusTrailers trailers = new GrpcStatusTrailers();

	private final GrpcHttpMessageWriter writer = new GrpcHttpMessageWriter(new GrpcEncoder(),
			(response) -> this.trailers);

	private final MockServerHttpResponse response = new MockServerHttpResponse();

	@Test
	void canWriteMessagesWithGrpcContentType() {
		assertThat(this.writer.canWrite(REPLY_TYPE, GrpcMediaTypes.APPLICATION_GRPC)).isTrue();
		assertThat(this.writer.getWritableMediaTypes()).containsExactly(GrpcMediaTypes.APPLICATION_GRPC,
				GrpcMediaTypes.APPLICATION_GRPC_PROTO);
	}

	@Test
	void cannotWriteWithoutGrpcContentType() {
		assertThat(this.writer.canWrite(REPLY_TYPE, null)).isFalse();
		assertThat(this.writer.canWrite(REPLY_TYPE, MediaType.valueOf("application/x-protobuf"))).isFalse();
	}

	@Test
	void writeSingleMessageAsStreamFollowedByOkStatus() {
		StepVerifier.create(write(Mono.just(HELLO), GrpcMediaTypes.APPLICATION_GRPC)).verifyComplete();

		assertThat(body()).containsExactly(frame(HELLO));
		assertThat(this.response.getHeaders().getContentType()).isEqualTo(GrpcMediaTypes.APPLICATION_GRPC);
		assertThat(this.response.getHeaders().getContentLength()).isEqualTo(-1);
		assertThat(sentStatus().getCode()).isEqualTo(Status.Code.OK);
	}

	@Test
	void writeSeveralMessagesFollowedByOkStatus() {
		StepVerifier.create(write(Flux.just(HELLO, GOODBYE), GrpcMediaTypes.APPLICATION_GRPC)).verifyComplete();

		assertThat(body()).containsExactly(concat(frame(HELLO), frame(GOODBYE)));
		assertThat(this.response.getHeaders().getContentType()).isEqualTo(GrpcMediaTypes.APPLICATION_GRPC);
		assertThat(sentStatus().getCode()).isEqualTo(Status.Code.OK);
	}

	@Test
	void writeNoMessagesStillSendsOkStatus() {
		StepVerifier.create(write(Flux.empty(), GrpcMediaTypes.APPLICATION_GRPC)).verifyComplete();

		assertThat(body()).isEmpty();
		assertThat(sentStatus().getCode()).isEqualTo(Status.Code.OK);
	}

	@Test
	void writeKeepsRequestedGrpcContentType() {
		StepVerifier.create(write(Flux.just(HELLO), GrpcMediaTypes.APPLICATION_GRPC_PROTO)).verifyComplete();

		assertThat(this.response.getHeaders().getContentType()).isEqualTo(GrpcMediaTypes.APPLICATION_GRPC_PROTO);
	}

	@Test
	void writeWithoutContentTypeUsesGrpcContentType() {
		StepVerifier.create(write(Flux.just(HELLO), null)).verifyComplete();

		assertThat(this.response.getHeaders().getContentType()).isEqualTo(GrpcMediaTypes.APPLICATION_GRPC);
	}

	@Test
	void writeFailsBeforeAnyMessageWhenTrailersAreNotSupported() {
		GrpcHttpMessageWriter writer = new GrpcHttpMessageWriter();

		StepVerifier
			.create(writer.write(Flux.just(HELLO), REPLY_TYPE, GrpcMediaTypes.APPLICATION_GRPC, this.response,
					Map.of()))
			.expectErrorSatisfies((ex) -> assertThat(ex).isInstanceOf(IllegalStateException.class)
				.hasMessageContaining("Reactor Netty"))
			.verify();
		assertThat(this.response.isCommitted()).isFalse();
	}

	@Test
	void errorWithStatusIsSentAsThatStatus() {
		Status notFound = Status.NOT_FOUND.withDescription("No such greeting");

		StepVerifier.create(write(Mono.error(notFound.asRuntimeException()), GrpcMediaTypes.APPLICATION_GRPC))
			.verifyComplete();

		assertThat(body()).isEmpty();
		assertThat(sentStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND);
		assertThat(sentStatus().getDescription()).isEqualTo("No such greeting");
	}

	@Test
	void errorAfterSomeMessagesIsSentAfterThem() {
		Flux<HelloReply> replies = Flux.just(HELLO).concatWith(Mono.error(Status.ABORTED.asException()));

		StepVerifier.create(write(replies, GrpcMediaTypes.APPLICATION_GRPC)).verifyComplete();

		assertThat(body()).containsExactly(frame(HELLO));
		assertThat(sentStatus().getCode()).isEqualTo(Status.Code.ABORTED);
	}

	@Test
	void statusExceptionNestedInCauseIsSentAsThatStatus() {
		RuntimeException ex = new IllegalStateException("wrapper", Status.PERMISSION_DENIED.asException());

		StepVerifier.create(write(Mono.error(ex), GrpcMediaTypes.APPLICATION_GRPC)).verifyComplete();

		assertThat(sentStatus().getCode()).isEqualTo(Status.Code.PERMISSION_DENIED);
	}

	@Test
	void errorWithOkStatusIsSentAsUnknown() {
		StepVerifier.create(write(Mono.error(Status.OK.asRuntimeException()), GrpcMediaTypes.APPLICATION_GRPC))
			.verifyComplete();

		assertThat(sentStatus().getCode()).isEqualTo(Status.Code.UNKNOWN);
	}

	@Test
	void requestThatCannotBeDecodedIsSentAsTheDecoderStatus() {
		DecodingException ex = new GrpcDecodingException(Status.Code.RESOURCE_EXHAUSTED, "gRPC message too large");

		StepVerifier.create(write(Mono.error(ex), GrpcMediaTypes.APPLICATION_GRPC)).verifyComplete();

		assertThat(sentStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
		assertThat(sentStatus().getDescription()).isEqualTo("gRPC message too large");
	}

	@Test
	void decodingFailureFromAnotherSourceIsSentAsUnknownWithoutDetails() {
		DecodingException ex = new DecodingException("JSON decoding error: {\"secret\":42}");

		StepVerifier.create(write(Mono.error(ex), GrpcMediaTypes.APPLICATION_GRPC)).verifyComplete();

		assertThat(sentStatus().getCode()).isEqualTo(Status.Code.UNKNOWN);
		assertThat(sentStatus().getDescription()).isNull();
	}

	@Test
	void sizeLimitFromAnotherSourceIsSentAsUnknownWithoutDetails() {
		DataBufferLimitException ex = new DataBufferLimitException("Exceeded limit on max bytes to buffer");

		StepVerifier.create(write(Mono.error(ex), GrpcMediaTypes.APPLICATION_GRPC)).verifyComplete();

		assertThat(sentStatus().getCode()).isEqualTo(Status.Code.UNKNOWN);
		assertThat(sentStatus().getDescription()).isNull();
	}

	@Test
	void unexpectedErrorIsSentAsUnknownWithoutDetails() {
		RuntimeException ex = new IllegalStateException("Internal details");

		StepVerifier.create(write(Mono.error(ex), GrpcMediaTypes.APPLICATION_GRPC)).verifyComplete();

		assertThat(sentStatus().getCode()).isEqualTo(Status.Code.UNKNOWN);
		assertThat(sentStatus().getDescription()).isNull();
	}

	private Mono<Void> write(Publisher<? extends Message> messages, @Nullable MediaType mediaType) {
		return this.writer.write(messages, REPLY_TYPE, mediaType, this.response, Map.of());
	}

	private Status sentStatus() {
		Status status = this.trailers.getStatus();
		assertThat(status).isNotNull();
		return status;
	}

	private byte[] body() {
		return DataBufferUtils.join(this.response.getBody()).map(GrpcFrames::bytes).defaultIfEmpty(new byte[0]).block();
	}

}
