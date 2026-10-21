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

import java.util.Map;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;

import org.springframework.core.ResolvableType;
import org.springframework.core.codec.Encoder;
import org.springframework.http.MediaType;
import org.springframework.http.ReactiveHttpOutputMessage;
import org.springframework.http.codec.protobuf.ProtobufHttpMessageWriter;
import org.springframework.http.server.reactive.ServerHttpResponse;

import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * An {@code HttpMessageWriter} that writes Protobuf {@link Message Messages} as the body
 * of a gRPC response and then sends the status of the call in its trailers.
 * <p>
 * The messages are always written as a stream, even a single message, so that the
 * response has no {@code Content-Length} and each message is flushed as soon as it is
 * written. When the messages complete, the status is {@link Status#OK OK}. When they
 * fail, the error is {@link #resolveStatus(Throwable) resolved to a status} and the
 * response still completes normally, so that the client receives that status, even after
 * some messages have been written.
 * <p>
 * The status is sent in {@link GrpcStatusTrailers trailers}, which only Reactor Netty
 * supports. On other servers, writing fails before any message is written.
 *
 * @author Dave Syer
 * @author Sharang Gupta
 * @since 1.1.2
 * @see GrpcStatusTrailers
 */
public class GrpcHttpMessageWriter extends ProtobufHttpMessageWriter {

	private static final Log logger = LogFactory.getLog(GrpcHttpMessageWriter.class);

	private final Function<ServerHttpResponse, GrpcStatusTrailers> trailersRegistrar;

	/**
	 * Create a new {@code GrpcHttpMessageWriter} with a default {@link GrpcEncoder}.
	 */
	public GrpcHttpMessageWriter() {
		this(new GrpcEncoder());
	}

	/**
	 * Create a new {@code GrpcHttpMessageWriter} with the given encoder.
	 * @param encoder the gRPC message encoder to use
	 */
	public GrpcHttpMessageWriter(Encoder<Message> encoder) {
		this(encoder, GrpcStatusTrailers::register);
	}

	GrpcHttpMessageWriter(Encoder<Message> encoder,
			Function<ServerHttpResponse, GrpcStatusTrailers> trailersRegistrar) {
		super(encoder);
		this.trailersRegistrar = trailersRegistrar;
	}

	@Override
	public Mono<Void> write(Publisher<? extends Message> inputStream, ResolvableType elementType,
			@Nullable MediaType mediaType, ReactiveHttpOutputMessage message, Map<String, Object> hints) {
		if (!(message instanceof ServerHttpResponse response)) {
			return super.write(inputStream, elementType, mediaType, message, hints);
		}
		GrpcStatusTrailers trailers;
		try {
			trailers = this.trailersRegistrar.apply(response);
		}
		catch (IllegalStateException ex) {
			return Mono.error(ex);
		}
		Flux<Message> messagesThenStatus = Flux.<Message>from(inputStream)
			.concatWith(Mono.fromRunnable(() -> trailers.setStatus(Status.OK)))
			.onErrorResume((ex) -> {
				trailers.setStatus(resolveFailureStatus(ex));
				return Mono.empty();
			});
		return super.write(messagesThenStatus, elementType, mediaType, message, hints);
	}

	@Override
	protected MediaType getStreamingContentType(@Nullable MediaType mediaType) {
		return (mediaType != null) ? mediaType : GrpcMediaTypes.APPLICATION_GRPC;
	}

	/**
	 * Resolve the status to send when the messages of a response fail with the given
	 * error.
	 * <p>
	 * By default, a request message that the {@link GrpcDecoder} could not decode gives
	 * the status chosen by the decoder, a {@link StatusException} or
	 * {@link StatusRuntimeException} in the causes of the error gives its status, and any
	 * other error gives {@link Status#UNKNOWN} with no description, so that no details of
	 * unexpected errors reach the client.
	 * @param ex the error
	 * @return the status to send, never {@link Status#OK OK}
	 */
	protected Status resolveStatus(Throwable ex) {
		GrpcDecodingException decodingFailure = findCause(ex, GrpcDecodingException.class);
		if (decodingFailure != null) {
			return decodingFailure.getStatus().withCause(ex);
		}
		Status status = Status.fromThrowable(ex);
		return status.isOk() ? Status.UNKNOWN.withCause(ex) : status;
	}

	private Status resolveFailureStatus(Throwable ex) {
		Status status = resolveStatus(ex);
		if (status.getCode() == Status.Code.UNKNOWN) {
			logger.warn("Completing gRPC response with status UNKNOWN after unexpected error", ex);
		}
		else if (logger.isDebugEnabled()) {
			logger.debug("Completing gRPC response with status " + status.getCode() + " after error", ex);
		}
		return status;
	}

	private static <T extends Throwable> @Nullable T findCause(Throwable ex, Class<T> type) {
		for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
			if (type.isInstance(cause)) {
				return type.cast(cause);
			}
		}
		return null;
	}

}
