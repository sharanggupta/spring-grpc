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
import static org.springframework.grpc.web.reactive.GrpcFrames.buffer;
import static org.springframework.grpc.web.reactive.GrpcFrames.concat;
import static org.springframework.grpc.web.reactive.GrpcFrames.frame;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.springframework.core.ResolvableType;
import org.springframework.core.codec.DecodingException;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.http.MediaType;
import org.springframework.util.MimeType;

import com.example.hello.HelloReply;
import com.example.hello.HelloRequest;
import com.google.protobuf.Message;
import io.grpc.Status;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Tests for {@link GrpcDecoder}.
 */
class GrpcDecoderTests {

	private static final ResolvableType REQUEST_TYPE = ResolvableType.forClass(HelloRequest.class);

	private static final HelloRequest ALIEN = HelloRequest.newBuilder().setName("Alien").build();

	private static final HelloRequest PREDATOR = HelloRequest.newBuilder().setName("Predator").build();

	private final GrpcDecoder decoder = new GrpcDecoder();

	@Test
	void canDecodeMessagesWithGrpcContentTypes() {
		assertThat(this.decoder.canDecode(REQUEST_TYPE, GrpcMediaTypes.APPLICATION_GRPC)).isTrue();
		assertThat(this.decoder.canDecode(REQUEST_TYPE, GrpcMediaTypes.APPLICATION_GRPC_PROTO)).isTrue();
		assertThat(this.decoder.canDecode(ResolvableType.forClass(HelloReply.class),
				MimeType.valueOf("application/grpc;charset=utf-8")))
			.isTrue();
	}

	@Test
	void cannotDecodeWithoutGrpcContentType() {
		assertThat(this.decoder.canDecode(REQUEST_TYPE, null)).isFalse();
		assertThat(this.decoder.canDecode(REQUEST_TYPE, MediaType.valueOf("application/x-protobuf"))).isFalse();
		assertThat(this.decoder.canDecode(REQUEST_TYPE, MediaType.valueOf("application/grpc+json"))).isFalse();
	}

	@Test
	void cannotDecodeTypesThatAreNotMessages() {
		assertThat(this.decoder.canDecode(ResolvableType.forClass(String.class), GrpcMediaTypes.APPLICATION_GRPC))
			.isFalse();
	}

	@Test
	void decodableMimeTypesAreTheGrpcContentTypes() {
		assertThat(this.decoder.getDecodableMimeTypes()).containsExactly(GrpcMediaTypes.APPLICATION_GRPC,
				GrpcMediaTypes.APPLICATION_GRPC_PROTO);
	}

	@Test
	void decodeSingleMessage() {
		StepVerifier.create(decode(buffer(frame(ALIEN)))).expectNext(ALIEN).verifyComplete();
	}

	@Test
	void decodeSeveralMessagesPackedIntoOneBuffer() {
		StepVerifier.create(decode(buffer(frame(ALIEN), frame(PREDATOR), frame(ALIEN))))
			.expectNext(ALIEN, PREDATOR, ALIEN)
			.verifyComplete();
	}

	@Test
	void decodeMessageSplitAcrossBuffersAtEveryPosition() {
		byte[] frame = frame(PREDATOR);
		for (int split = 1; split < frame.length; split++) {
			byte[] first = Arrays.copyOfRange(frame, 0, split);
			byte[] second = Arrays.copyOfRange(frame, split, frame.length);
			StepVerifier.create(decode(buffer(first), buffer(second)))
				.as("split at byte " + split)
				.expectNext(PREDATOR)
				.verifyComplete();
		}
	}

	@Test
	void decodeMessagesDeliveredOneByteAtATime() {
		byte[] bytes = concat(frame(ALIEN), frame(HelloRequest.getDefaultInstance()), frame(PREDATOR));
		List<DataBuffer> buffers = new ArrayList<>();
		for (byte b : bytes) {
			buffers.add(buffer(new byte[] { b }));
		}
		StepVerifier.create(decode(buffers.toArray(DataBuffer[]::new)))
			.expectNext(ALIEN, HelloRequest.getDefaultInstance(), PREDATOR)
			.verifyComplete();
	}

	@Test
	void decodeZeroLengthMessage() {
		byte[] emptyFrame = { 0, 0, 0, 0, 0 };
		StepVerifier.create(decode(buffer(emptyFrame))).expectNext(HelloRequest.getDefaultInstance()).verifyComplete();
	}

	@Test
	void decodeZeroLengthMessageBetweenOtherMessages() {
		byte[] emptyFrame = { 0, 0, 0, 0, 0 };
		StepVerifier.create(decode(buffer(frame(ALIEN), emptyFrame, frame(PREDATOR))))
			.expectNext(ALIEN, HelloRequest.getDefaultInstance(), PREDATOR)
			.verifyComplete();
	}

	@Test
	void decodeEmptyBodyHasNoMessages() {
		StepVerifier.create(decode()).verifyComplete();
		StepVerifier.create(decode(buffer(new byte[0]))).verifyComplete();
	}

	@Test
	void decodeRejectsCompressedMessage() {
		byte[] compressed = frame(ALIEN);
		compressed[0] = 1;
		StepVerifier.create(decode(buffer(compressed)))
			.expectErrorSatisfies((ex) -> assertFailure(ex, Status.Code.INTERNAL, "compressed"))
			.verify();
	}

	@Test
	void decodeRejectsUnknownFlagInMessageHeader() {
		byte[] malformed = frame(ALIEN);
		malformed[0] = 2;
		StepVerifier.create(decode(buffer(malformed)))
			.expectErrorSatisfies((ex) -> assertFailure(ex, Status.Code.INTERNAL, "flag 2"))
			.verify();
	}

	@Test
	void decodeMessageOfExactlyMaxMessageSize() {
		this.decoder.setMaxMessageSize(PREDATOR.getSerializedSize());
		StepVerifier.create(decode(buffer(frame(PREDATOR)))).expectNext(PREDATOR).verifyComplete();
	}

	@Test
	void decodeRejectsMessageLargerThanMaxMessageSize() {
		this.decoder.setMaxMessageSize(PREDATOR.getSerializedSize() - 1);
		StepVerifier.create(decode(buffer(frame(PREDATOR))))
			.expectErrorSatisfies((ex) -> assertMessageTooLarge(ex))
			.verify();
	}

	@Test
	void decodeRejectsMessageLengthBeyondIntegerRange() {
		byte[] header = { 0, (byte) 0x80, 0, 0, 0 };
		this.decoder.setMaxMessageSize(-1);
		StepVerifier.create(decode(buffer(header)))
			.expectErrorSatisfies((ex) -> assertFailure(ex, Status.Code.RESOURCE_EXHAUSTED, "2147483648"))
			.verify();
	}

	@Test
	void decodeRejectsInvalidProtobufWithoutExposingDetails() {
		byte[] invalid = { 0, 0, 0, 0, 2, (byte) 0xFF, (byte) 0xFF };
		StepVerifier.create(decode(buffer(invalid)))
			.expectErrorSatisfies((ex) -> assertFailure(ex, Status.Code.INTERNAL, "Invalid protobuf byte sequence"))
			.verify();
	}

	@Test
	void decodeToMonoSingleMessage() {
		StepVerifier.create(decodeToMono(buffer(frame(ALIEN)))).expectNext(ALIEN).verifyComplete();
	}

	@Test
	void decodeToMonoMessageSplitAcrossBuffers() {
		byte[] frame = frame(PREDATOR);
		StepVerifier
			.create(decodeToMono(buffer(Arrays.copyOfRange(frame, 0, 3)), buffer(Arrays.copyOfRange(frame, 3, 7)),
					buffer(Arrays.copyOfRange(frame, 7, frame.length))))
			.expectNext(PREDATOR)
			.verifyComplete();
	}

	@Test
	void decodeToMonoZeroLengthMessage() {
		StepVerifier.create(decodeToMono(buffer(new byte[] { 0, 0, 0, 0, 0 })))
			.expectNext(HelloRequest.getDefaultInstance())
			.verifyComplete();
	}

	@Test
	void decodeToMonoEmptyBodyHasNoMessage() {
		StepVerifier.create(decodeToMono()).verifyComplete();
		StepVerifier.create(decodeToMono(buffer(new byte[0]))).verifyComplete();
	}

	@Test
	void decodeToMonoRejectsCompressedMessage() {
		byte[] compressed = frame(ALIEN);
		compressed[0] = 1;
		StepVerifier.create(decodeToMono(buffer(compressed)))
			.expectErrorSatisfies((ex) -> assertFailure(ex, Status.Code.INTERNAL, "compressed"))
			.verify();
	}

	@Test
	void decodeToMonoRejectsMoreThanOneMessage() {
		StepVerifier.create(decodeToMono(buffer(frame(ALIEN), frame(PREDATOR))))
			.expectErrorSatisfies((ex) -> assertFailure(ex, Status.Code.INTERNAL, "single gRPC message"))
			.verify();
	}

	@Test
	void decodeToMonoRejectsTruncatedMessage() {
		byte[] frame = frame(PREDATOR);
		StepVerifier.create(decodeToMono(buffer(Arrays.copyOfRange(frame, 0, frame.length - 1))))
			.expectErrorSatisfies((ex) -> assertFailure(ex, Status.Code.INTERNAL, "single gRPC message"))
			.verify();
	}

	@Test
	void decodeToMonoRejectsIncompleteHeader() {
		StepVerifier.create(decodeToMono(buffer(new byte[] { 0, 0, 0 })))
			.expectErrorSatisfies((ex) -> assertFailure(ex, Status.Code.INTERNAL, "header"))
			.verify();
	}

	@Test
	void decodeToMonoMessageOfExactlyMaxMessageSize() {
		this.decoder.setMaxMessageSize(PREDATOR.getSerializedSize());
		StepVerifier.create(decodeToMono(buffer(frame(PREDATOR)))).expectNext(PREDATOR).verifyComplete();
	}

	@Test
	void decodeToMonoRejectsMessageLargerThanMaxMessageSize() {
		this.decoder.setMaxMessageSize(PREDATOR.getSerializedSize() - 1);
		StepVerifier.create(decodeToMono(buffer(frame(PREDATOR))))
			.expectErrorSatisfies((ex) -> assertMessageTooLarge(ex))
			.verify();
	}

	@Test
	void decodeToMonoWithUnlimitedMessageSize() {
		this.decoder.setMaxMessageSize(-1);
		StepVerifier.create(decodeToMono(buffer(frame(PREDATOR)))).expectNext(PREDATOR).verifyComplete();
	}

	@Test
	void decodeSingleDataBuffer() {
		Message message = this.decoder.decode(buffer(frame(ALIEN)), REQUEST_TYPE, GrpcMediaTypes.APPLICATION_GRPC,
				null);
		assertThat(message).isEqualTo(ALIEN);
	}

	private Flux<Message> decode(DataBuffer... buffers) {
		return this.decoder.decode(Flux.just(buffers), REQUEST_TYPE, GrpcMediaTypes.APPLICATION_GRPC, null);
	}

	private Mono<Message> decodeToMono(DataBuffer... buffers) {
		return this.decoder.decodeToMono(Flux.just(buffers), REQUEST_TYPE, GrpcMediaTypes.APPLICATION_GRPC, null);
	}

	private static void assertFailure(Throwable ex, Status.Code code, String description) {
		assertThat(ex).isInstanceOf(DecodingException.class)
			.isInstanceOfSatisfying(GrpcDecodingException.class, (failure) -> {
				assertThat(failure.getStatus().getCode()).isEqualTo(code);
				assertThat(failure.getStatus().getDescription()).contains(description);
			});
	}

	private static void assertMessageTooLarge(Throwable ex) {
		assertFailure(ex, Status.Code.RESOURCE_EXHAUSTED, "too large");
		assertThat(ex).hasRootCauseInstanceOf(DataBufferLimitException.class);
	}

}
