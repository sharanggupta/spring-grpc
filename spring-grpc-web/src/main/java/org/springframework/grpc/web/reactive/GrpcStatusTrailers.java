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

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

import org.jspecify.annotations.Nullable;

import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.util.ClassUtils;

import io.grpc.Status;
import reactor.netty.http.server.HttpServerResponse;

/**
 * The status of a gRPC call, sent in the HTTP trailers of a {@link ServerHttpResponse}:
 * the {@value #GRPC_STATUS} code and, when the status has a description, the
 * percent-encoded {@value #GRPC_MESSAGE}.
 * <p>
 * The trailers are {@link #register(ServerHttpResponse) registered} with the response
 * before its body is written, and are written when the response completes with the status
 * {@link #setStatus(Status) set} by then. That needs support from the underlying server:
 * only Reactor Netty is supported, and the client must use HTTP/2.
 *
 * @author Dave Syer
 * @author Sharang Gupta
 * @since 1.1.2
 * @see <a href=
 * "https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#responses">gRPC
 * responses</a>
 */
public final class GrpcStatusTrailers {

	/**
	 * The name of the trailer that holds the status code.
	 */
	public static final String GRPC_STATUS = "grpc-status";

	/**
	 * The name of the trailer that holds the status description.
	 */
	public static final String GRPC_MESSAGE = "grpc-message";

	private static final boolean reactorNettyPresent = ClassUtils
		.isPresent("reactor.netty.http.server.HttpServerResponse", GrpcStatusTrailers.class.getClassLoader());

	private static final char[] HEX_DIGITS = "0123456789ABCDEF".toCharArray();

	private volatile @Nullable Status status;

	GrpcStatusTrailers() {
	}

	/**
	 * Register trailers with the given response, so that the status they hold when the
	 * response completes is sent to the client. Without a status, no trailers are sent.
	 * @param response the response, before its body is written
	 * @return the trailers to set the status on
	 * @throws IllegalStateException if the response is not served by Reactor Netty
	 */
	public static GrpcStatusTrailers register(ServerHttpResponse response) {
		GrpcStatusTrailers trailers = new GrpcStatusTrailers();
		Object nativeResponse = getNativeResponse(response);
		if (reactorNettyPresent && ReactorNettyTrailers.register(nativeResponse, trailers)) {
			return trailers;
		}
		throw new IllegalStateException("Cannot send gRPC status trailers with " + response.getClass().getName()
				+ ": only responses served by Reactor Netty are supported");
	}

	/**
	 * Set the status to send, replacing any status set earlier.
	 * @param status the status of the gRPC call
	 */
	public void setStatus(Status status) {
		this.status = status;
	}

	/**
	 * Return the status to send.
	 * @return the status, or {@code null} if none has been set
	 */
	public @Nullable Status getStatus() {
		return this.status;
	}

	/**
	 * Return the trailers that represent the status set so far.
	 * @return the trailer values by name, empty if no status has been set
	 */
	Map<String, String> toTrailers() {
		Status current = this.status;
		return (current != null) ? toTrailers(current) : Map.of();
	}

	/**
	 * Return the trailers that represent the given status.
	 * @param status the status of the gRPC call
	 * @return the trailer values by name
	 */
	static Map<String, String> toTrailers(Status status) {
		Map<String, String> trailers = new LinkedHashMap<>();
		trailers.put(GRPC_STATUS, String.valueOf(status.getCode().value()));
		String description = status.getDescription();
		if (description != null) {
			trailers.put(GRPC_MESSAGE, percentEncode(description));
		}
		return trailers;
	}

	/**
	 * Percent-encode the UTF-8 bytes of the given message, leaving printable ASCII
	 * characters other than {@code %} as they are.
	 * @param message the status description
	 * @return the encoded message
	 */
	private static String percentEncode(String message) {
		StringBuilder encoded = new StringBuilder(message.length());
		for (byte b : message.getBytes(StandardCharsets.UTF_8)) {
			if (b >= ' ' && b <= '~' && b != '%') {
				encoded.append((char) b);
			}
			else {
				encoded.append('%').append(HEX_DIGITS[(b >> 4) & 0xF]).append(HEX_DIGITS[b & 0xF]);
			}
		}
		return encoded.toString();
	}

	private static @Nullable Object getNativeResponse(ServerHttpResponse response) {
		try {
			return ServerHttpResponseDecorator.getNativeResponse(response);
		}
		catch (IllegalArgumentException | IllegalStateException ex) {
			// Not backed by a server, for example a mock response
			return null;
		}
	}

	/**
	 * Registers trailers with Reactor Netty, kept separate so that the rest of the class
	 * works without it.
	 */
	private static final class ReactorNettyTrailers {

		static boolean register(@Nullable Object nativeResponse, GrpcStatusTrailers trailers) {
			if (nativeResponse instanceof HttpServerResponse serverResponse) {
				serverResponse.trailerHeaders((headers) -> trailers.toTrailers().forEach(headers::set));
				return true;
			}
			return false;
		}

	}

}
