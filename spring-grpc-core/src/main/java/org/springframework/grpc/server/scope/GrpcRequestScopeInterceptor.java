/*
 * Copyright 2024-present the original author or authors.
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

package org.springframework.grpc.server.scope;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.Scope;
import org.springframework.core.Ordered;
import org.springframework.util.Assert;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

/**
 * A {@link Scope} that lasts for the duration of a gRPC call. Register it as a
 * {@link org.springframework.grpc.server.GlobalServerInterceptor global server
 * interceptor} bean: it registers itself with the bean factory under the
 * {@value #SCOPE_NAME} scope name and opens a new scope for every call it intercepts,
 * ahead of all other interceptors. Beans in this scope are created on first use within a
 * call, are shared by all threads to which the call's {@link Context gRPC context} is
 * propagated, and are destroyed when the call completes or is cancelled.
 * <p>
 * Annotate beans with {@link GrpcRequestScope @GrpcRequestScope} to place them in this
 * scope.
 *
 * @author Sharang Gupta
 * @since 1.1.2
 * @see GrpcRequestScope
 */
public class GrpcRequestScopeInterceptor implements Scope, ServerInterceptor, BeanFactoryPostProcessor, Ordered {

	/**
	 * The name of the scope.
	 */
	public static final String SCOPE_NAME = "grpcRequest";

	/**
	 * The order of the interceptor: it runs before all other interceptors so that scoped
	 * beans are available to them.
	 */
	public static final int ORDER = Ordered.HIGHEST_PRECEDENCE;

	private static final Log logger = LogFactory.getLog(GrpcRequestScopeInterceptor.class);

	private static final Context.Key<ScopedBeans> SCOPED_BEANS = Context.key("grpcRequestScope");

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
		beanFactory.registerScope(SCOPE_NAME, this);
	}

	@Override
	public int getOrder() {
		return ORDER;
	}

	@Override
	public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
			ServerCallHandler<ReqT, RespT> next) {
		ScopedBeans scopedBeans = new ScopedBeans();
		Context context = Context.current().withValue(SCOPED_BEANS, scopedBeans);
		context.addListener((cancelled) -> scopedBeans.destroy(), Runnable::run);
		return Contexts.interceptCall(context, call, headers, next);
	}

	@Override
	public Object get(String name, ObjectFactory<?> objectFactory) {
		return currentScopedBeans().get(name, objectFactory);
	}

	@Override
	public @Nullable Object remove(String name) {
		return currentScopedBeans().remove(name);
	}

	@Override
	public void registerDestructionCallback(String name, Runnable callback) {
		currentScopedBeans().registerDestructionCallback(name, callback);
	}

	@Override
	public @Nullable Object resolveContextualObject(String key) {
		return null;
	}

	@Override
	public @Nullable String getConversationId() {
		return null;
	}

	private static ScopedBeans currentScopedBeans() {
		ScopedBeans scopedBeans = SCOPED_BEANS.get();
		Assert.state(scopedBeans != null,
				() -> "No gRPC call is active in the current context: the " + SCOPE_NAME
						+ " scope is only available within a call intercepted by GrpcRequestScopeInterceptor"
						+ " and in threads to which the call's io.grpc.Context has been propagated");
		return scopedBeans;
	}

	/**
	 * The beans of one call together with their destruction callbacks.
	 */
	private static final class ScopedBeans {

		private final Map<String, Object> beans = new LinkedHashMap<>();

		private final Map<String, Runnable> destructionCallbacks = new LinkedHashMap<>();

		synchronized Object get(String name, ObjectFactory<?> objectFactory) {
			Object bean = this.beans.get(name);
			if (bean == null) {
				bean = objectFactory.getObject();
				this.beans.put(name, bean);
			}
			return bean;
		}

		synchronized @Nullable Object remove(String name) {
			this.destructionCallbacks.remove(name);
			return this.beans.remove(name);
		}

		synchronized void registerDestructionCallback(String name, Runnable callback) {
			this.destructionCallbacks.put(name, callback);
		}

		void destroy() {
			Map<String, Runnable> callbacks;
			synchronized (this) {
				callbacks = new LinkedHashMap<>(this.destructionCallbacks);
				this.destructionCallbacks.clear();
				this.beans.clear();
			}
			callbacks.forEach((name, callback) -> {
				try {
					callback.run();
				}
				catch (Throwable ex) {
					logger.error("Destruction callback of gRPC request scoped bean '" + name + "' failed", ex);
				}
			});
		}

	}

}
