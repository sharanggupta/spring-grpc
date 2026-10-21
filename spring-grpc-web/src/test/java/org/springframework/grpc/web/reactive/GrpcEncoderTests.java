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
import static org.springframework.grpc.web.reactive.GrpcFrames.bytes;
import static org.springframework.grpc.web.reactive.GrpcFrames.frame;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import org.springframework.core.ResolvableType;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.http.MediaType;

import com.example.hello.HelloReply;
import com.google.protobuf.Message;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Tests for {@link GrpcEncoder}.
 */
class GrpcEncoderTests {

	private static final ResolvableType REPLY_TYPE = ResolvableType.forClass(HelloReply.class);

	private static final HelloReply HELLO = HelloReply.newBuilder().setMessage("Hello").build();

	private static final HelloReply GOODBYE = HelloReply.newBuilder().setMessage("Goodbye").build();

	private final GrpcEncoder encoder = new GrpcEncoder();

	@Test
	void canEncodeMessagesWithGrpcContentTypes() {
		assertThat(this.encoder.canEncode(REPLY_TYPE, GrpcMediaTypes.APPLICATION_GRPC)).isTrue();
		assertThat(this.encoder.canEncode(REPLY_TYPE, GrpcMediaTypes.APPLICATION_GRPC_PROTO)).isTrue();
	}

	@Test
	void cannotEncodeWithoutGrpcContentType() {
		assertThat(this.encoder.canEncode(REPLY_TYPE, null)).isFalse();
		assertThat(this.encoder.canEncode(REPLY_TYPE, MediaType.valueOf("application/x-protobuf"))).isFalse();
		assertThat(this.encoder.canEncode(REPLY_TYPE, MediaType.APPLICATION_JSON)).isFalse();
	}

	@Test
	void cannotEncodeTypesThatAreNotMessages() {
		assertThat(this.encoder.canEncode(ResolvableType.forClass(String.class), GrpcMediaTypes.APPLICATION_GRPC))
			.isFalse();
	}

	@Test
	void encodableMimeTypesAreTheGrpcContentTypes() {
		assertThat(this.encoder.getEncodableMimeTypes()).containsExactly(GrpcMediaTypes.APPLICATION_GRPC,
				GrpcMediaTypes.APPLICATION_GRPC_PROTO);
	}

	@Test
	void everyGrpcContentTypeIsStreamedWithoutParameters() {
		assertThat(this.encoder.getStreamingMediaTypes()).containsExactly(GrpcMediaTypes.APPLICATION_GRPC,
				GrpcMediaTypes.APPLICATION_GRPC_PROTO);
	}

	@Test
	void encodeValueWritesLengthPrefixedMessage() {
		DataBuffer buffer = this.encoder.encodeValue(HELLO, DefaultDataBufferFactory.sharedInstance, REPLY_TYPE,
				GrpcMediaTypes.APPLICATION_GRPC, null);
		assertThat(bytes(buffer)).containsExactly(frame(HELLO));
	}

	@Test
	void encodeZeroLengthMessage() {
		DataBuffer buffer = this.encoder.encodeValue(HelloReply.getDefaultInstance(),
				DefaultDataBufferFactory.sharedInstance, REPLY_TYPE, GrpcMediaTypes.APPLICATION_GRPC, null);
		assertThat(bytes(buffer)).containsExactly(0, 0, 0, 0, 0);
	}

	@Test
	void encodeMonoWritesOneLengthPrefixedMessage() {
		StepVerifier.create(encode(Mono.just(HELLO)))
			.assertNext((buffer) -> assertThat(bytes(buffer)).containsExactly(frame(HELLO)))
			.verifyComplete();
	}

	@Test
	void encodeFluxWritesEachMessageWithItsOwnPrefix() {
		StepVerifier.create(encode(Flux.just(HELLO, GOODBYE)))
			.assertNext((buffer) -> assertThat(bytes(buffer)).containsExactly(frame(HELLO)))
			.assertNext((buffer) -> assertThat(bytes(buffer)).containsExactly(frame(GOODBYE)))
			.verifyComplete();
	}

	@Test
	void encodedMessagesCanBeDecoded() {
		Flux<DataBuffer> encoded = encode(Flux.just(HELLO, HelloReply.getDefaultInstance(), GOODBYE));
		StepVerifier.create(new GrpcDecoder().decode(encoded, REPLY_TYPE, GrpcMediaTypes.APPLICATION_GRPC, null))
			.expectNext(HELLO, HelloReply.getDefaultInstance(), GOODBYE)
			.verifyComplete();
	}

	private Flux<DataBuffer> encode(Publisher<? extends Message> messages) {
		return this.encoder.encode(messages, DefaultDataBufferFactory.sharedInstance, REPLY_TYPE,
				GrpcMediaTypes.APPLICATION_GRPC, null);
	}

}
