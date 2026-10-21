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

import org.jspecify.annotations.Nullable;

import org.springframework.core.codec.DecodingException;

import io.grpc.Status;

/**
 * Thrown by {@link GrpcDecoder} when a gRPC request message cannot be decoded, with the
 * status to send to the client. Being a distinct type, it is never confused with a
 * {@link DecodingException} from some other decoder that a service used.
 *
 * @author Sharang Gupta
 */
final class GrpcDecodingException extends DecodingException {

	private final Status.Code code;

	GrpcDecodingException(Status.Code code, String description) {
		this(code, description, null);
	}

	GrpcDecodingException(Status.Code code, String description, @Nullable Throwable cause) {
		super(description, cause);
		this.code = code;
	}

	/**
	 * Return the status to send to the client.
	 * @return the status with this exception's message as its description
	 */
	Status getStatus() {
		return Status.fromCode(this.code).withDescription(getMessage());
	}

}
