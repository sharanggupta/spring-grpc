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

import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.http.MediaType;
import org.springframework.util.MimeType;

/**
 * Media types of gRPC requests and responses that carry Protobuf messages.
 *
 * @author Sharang Gupta
 * @since 1.1.2
 * @see <a href=
 * "https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md#requests">gRPC over
 * HTTP/2</a>
 */
public final class GrpcMediaTypes {

	/**
	 * A String equivalent of {@link #APPLICATION_GRPC}.
	 */
	public static final String APPLICATION_GRPC_VALUE = "application/grpc";

	/**
	 * The default gRPC media type, {@code application/grpc}.
	 */
	public static final MediaType APPLICATION_GRPC = MediaType.parseMediaType(APPLICATION_GRPC_VALUE);

	/**
	 * A String equivalent of {@link #APPLICATION_GRPC_PROTO}.
	 */
	public static final String APPLICATION_GRPC_PROTO_VALUE = "application/grpc+proto";

	/**
	 * The gRPC media type that names the Protobuf message format explicitly,
	 * {@code application/grpc+proto}.
	 */
	public static final MediaType APPLICATION_GRPC_PROTO = MediaType.parseMediaType(APPLICATION_GRPC_PROTO_VALUE);

	/**
	 * All gRPC media types for Protobuf messages, the default one first.
	 */
	static final List<MediaType> ALL = List.of(APPLICATION_GRPC, APPLICATION_GRPC_PROTO);

	private GrpcMediaTypes() {
	}

	/**
	 * Whether the given MIME type is one of the gRPC media types for Protobuf messages. A
	 * {@code null} MIME type is not, so that the gRPC framing is only ever used when a
	 * request or response explicitly declares it.
	 * @param mimeType the MIME type to check
	 * @return {@code true} if the MIME type is a gRPC media type
	 */
	static boolean isGrpc(@Nullable MimeType mimeType) {
		if (mimeType == null) {
			return false;
		}
		for (MediaType grpc : ALL) {
			if (grpc.isCompatibleWith(mimeType)) {
				return true;
			}
		}
		return false;
	}

}
