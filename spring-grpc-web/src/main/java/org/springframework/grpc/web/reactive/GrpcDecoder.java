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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;

import org.springframework.core.ResolvableType;
import org.springframework.core.codec.DecodingException;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.codec.protobuf.ProtobufDecoder;
import org.springframework.util.MimeType;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import io.grpc.Status;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * A {@code Decoder} that reads Protobuf {@link Message Messages} from a gRPC request
 * body, where each message is prefixed with a compressed flag and its length.
 * <p>
 * Requests are only decoded when their content type is one of the {@link GrpcMediaTypes
 * gRPC media types}. {@link #setMaxMessageSize(int) The maximum message size} applies to
 * each message, not counting its 5-byte prefix.
 * <p>
 * Compressed messages are rejected because this decoder does not support compression. A
 * message that cannot be decoded fails with a {@link DecodingException} that carries the
 * gRPC status to send to the client: {@code RESOURCE_EXHAUSTED} for a message larger than
 * the maximum size and {@code INTERNAL} otherwise.
 *
 * @author Dave Syer
 * @author Sharang Gupta
 * @since 1.1.2
 */
public class GrpcDecoder extends ProtobufDecoder {

	/**
	 * Create a new {@code GrpcDecoder}.
	 */
	public GrpcDecoder() {
		this(ExtensionRegistry.newInstance());
	}

	/**
	 * Create a new {@code GrpcDecoder} that uses the given registry of message
	 * extensions.
	 * @param extensionRegistry a message extension registry
	 */
	public GrpcDecoder(ExtensionRegistry extensionRegistry) {
		super(extensionRegistry);
		setMimeTypes(List.copyOf(GrpcMediaTypes.ALL));
	}

	@Override
	protected boolean supportsMimeType(@Nullable MimeType mimeType) {
		return GrpcMediaTypes.isGrpc(mimeType);
	}

	@Override
	protected MessageSizeReader initMessageSizeReader() {
		return new GrpcMessageSizeReader();
	}

	@Override
	public Flux<Message> decode(Publisher<DataBuffer> inputStream, ResolvableType elementType,
			@Nullable MimeType mimeType, @Nullable Map<String, Object> hints) {
		return super.decode(inputStream, elementType, mimeType, hints).onErrorMap(GrpcDecoder::isDecodingFailure,
				GrpcDecoder::withGrpcStatus);
	}

	@Override
	public Mono<Message> decodeToMono(Publisher<DataBuffer> inputStream, ResolvableType elementType,
			@Nullable MimeType mimeType, @Nullable Map<String, Object> hints) {
		return DataBufferUtils.join(inputStream, getMaxFrameSize())
			.filter((dataBuffer) -> dataBuffer.readableByteCount() > 0)
			.doOnDiscard(DataBuffer.class, DataBufferUtils::release)
			.map((dataBuffer) -> decode(dataBuffer, elementType, mimeType, hints))
			.onErrorMap(GrpcDecoder::isDecodingFailure, GrpcDecoder::withGrpcStatus);
	}

	/**
	 * Read a single message, which must take up the whole of the given buffer after its
	 * prefix.
	 */
	@Override
	protected void merge(DataBuffer dataBuffer, Message.Builder builder) throws IOException {
		Integer messageLength = initMessageSizeReader().readMessageSize(dataBuffer);
		if (messageLength == null) {
			throw new GrpcDecodingException(Status.Code.INTERNAL, "Incomplete gRPC message header: expected "
					+ GrpcMessageHeader.LENGTH + " bytes but got " + dataBuffer.readableByteCount());
		}
		if (dataBuffer.readableByteCount() != messageLength) {
			throw new GrpcDecodingException(Status.Code.INTERNAL, "Expected a single gRPC message of " + messageLength
					+ " bytes but got " + dataBuffer.readableByteCount() + " bytes");
		}
		super.merge(dataBuffer, builder);
	}

	/**
	 * Return the maximum size of a single message together with its prefix.
	 * @return the maximum number of bytes, or -1 if unlimited
	 */
	private int getMaxFrameSize() {
		int maxMessageSize = getMaxMessageSize();
		if (maxMessageSize < 0) {
			return maxMessageSize;
		}
		return (int) Math.min((long) maxMessageSize + GrpcMessageHeader.LENGTH, Integer.MAX_VALUE);
	}

	private static boolean isDecodingFailure(Throwable ex) {
		return ex instanceof DecodingException || ex instanceof DataBufferLimitException;
	}

	/**
	 * Give a decoding failure the gRPC status to send to the client, without exposing
	 * details of unexpected failures. {@link ProtobufDecoder} wraps the exceptions it
	 * catches, so the cause is checked as well as the failure itself.
	 * @param ex the decoding failure
	 * @return the failure with a gRPC status
	 */
	private static GrpcDecodingException withGrpcStatus(Throwable ex) {
		GrpcDecodingException grpcFailure = selfOrCause(ex, GrpcDecodingException.class);
		if (grpcFailure != null) {
			return grpcFailure;
		}
		DataBufferLimitException limitExceeded = selfOrCause(ex, DataBufferLimitException.class);
		if (limitExceeded != null) {
			return new GrpcDecodingException(Status.Code.RESOURCE_EXHAUSTED,
					"gRPC message too large: " + limitExceeded.getMessage(), ex);
		}
		return new GrpcDecodingException(Status.Code.INTERNAL, "Invalid protobuf byte sequence", ex);
	}

	private static <T extends Throwable> @Nullable T selfOrCause(Throwable ex, Class<T> type) {
		if (type.isInstance(ex)) {
			return type.cast(ex);
		}
		Throwable cause = ex.getCause();
		return type.isInstance(cause) ? type.cast(cause) : null;
	}

	/**
	 * Reads the prefix of each message, which may be split across buffers.
	 */
	private static final class GrpcMessageSizeReader implements MessageSizeReader {

		private final byte[] header = new byte[GrpcMessageHeader.LENGTH];

		private int headerBytesRead;

		@Override
		public @Nullable Integer readMessageSize(DataBuffer input) {
			int count = Math.min(this.header.length - this.headerBytesRead, input.readableByteCount());
			input.read(this.header, this.headerBytesRead, count);
			this.headerBytesRead += count;
			if (this.headerBytesRead < this.header.length) {
				return null;
			}
			this.headerBytesRead = 0;
			return GrpcMessageHeader.readMessageLength(this.header);
		}

	}

}
