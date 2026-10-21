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
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;
import static org.assertj.core.api.Assertions.entry;

import org.junit.jupiter.api.Test;

import org.springframework.mock.http.server.reactive.MockServerHttpResponse;

import io.grpc.Status;

/**
 * Tests for {@link GrpcStatusTrailers}.
 */
class GrpcStatusTrailersTests {

	@Test
	void okStatusHasOnlyStatusCode() {
		assertThat(GrpcStatusTrailers.toTrailers(Status.OK)).containsExactly(entry("grpc-status", "0"));
	}

	@Test
	void errorStatusHasStatusCodeAndMessage() {
		Status status = Status.NOT_FOUND.withDescription("No such greeting, sorry!");

		assertThat(GrpcStatusTrailers.toTrailers(status)).containsExactly(entry("grpc-status", "5"),
				entry("grpc-message", "No such greeting, sorry!"));
	}

	@Test
	void errorStatusWithoutDescriptionHasNoMessage() {
		assertThat(GrpcStatusTrailers.toTrailers(Status.UNAVAILABLE)).containsExactly(entry("grpc-status", "14"));
	}

	@Test
	void messageIsPercentEncodedOutsidePrintableAscii() {
		Status status = Status.INTERNAL.withDescription("100% Ünïcode\r\n~");

		assertThat(GrpcStatusTrailers.toTrailers(status)).containsEntry("grpc-message",
				"100%25 %C3%9Cn%C3%AFcode%0D%0A~");
	}

	@Test
	void trailersHoldTheLastStatusSet() {
		GrpcStatusTrailers trailers = new GrpcStatusTrailers();
		assertThat(trailers.toTrailers()).isEmpty();

		trailers.setStatus(Status.OK);
		trailers.setStatus(Status.ABORTED.withDescription("Stopped"));

		assertThat(trailers.toTrailers()).containsExactly(entry("grpc-status", "10"), entry("grpc-message", "Stopped"));
	}

	@Test
	void registerFailsForResponseNotServedByReactorNetty() {
		assertThatIllegalStateException().isThrownBy(() -> GrpcStatusTrailers.register(new MockServerHttpResponse()))
			.withMessageContaining("Reactor Netty");
	}

}
