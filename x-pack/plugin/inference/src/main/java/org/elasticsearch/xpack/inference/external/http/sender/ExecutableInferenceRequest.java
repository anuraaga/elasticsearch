/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.Request;

import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes;

final class ExecutableInferenceRequest implements Runnable {
    private final RequestSender requestSender;
    private final Logger logger;
    private final Request request;
    private final ResponseHandler responseHandler;
    private final Supplier<Boolean> hasFinished;
    private final ActionListener<InferenceServiceResults> listener;
    private final Context parentContext;

    ExecutableInferenceRequest(
        RequestSender requestSender,
        Logger logger,
        Request request,
        ResponseHandler responseHandler,
        Supplier<Boolean> hasFinished,
        ActionListener<InferenceServiceResults> listener
    ) {
        this.requestSender = requestSender;
        this.logger = logger;
        this.request = request;
        this.responseHandler = responseHandler;
        this.hasFinished = hasFinished;
        this.listener = listener;

        Context parent = null;
        ThreadPool threadPool = requestSender.threadPool();
        if (threadPool != null) {
            parent = GlobalOpenTelemetry.get().getPropagators()
                                  .getTextMapPropagator()
                                  .extract(Context.current(), threadPool.getThreadContext(), new TextMapGetter<>() {
                                      @Override
                                      public Iterable<String> keys(ThreadContext carrier) {
                                          return carrier.getHeaders().keySet();
                                      }

                                      @Override
                                      public String get(ThreadContext carrier, String key) {
                                          return carrier.getHeader(key);
                                      }
                                  });
        }
        System.out.println("parent");
        System.out.println(parent);
        parentContext = parent;

        new Exception().printStackTrace();
    }

    @Override
    public void run() {
        var inferenceEntityId = request.getInferenceEntityId();

        Model model = request.model2();
        ThreadPool threadPool = requestSender.threadPool();

        var openTelemetry = GlobalOpenTelemetry.get();
        var tracer = openTelemetry.getTracer("elasticsearch", Build.current().version());
        Span span;

        if (model != null && threadPool != null) {
            span = tracer.spanBuilder(model.genAiOperation() + " " + model.genAiRequestModel())
                         .setParent(parentContext)
                         .setAttribute(GenAiIncubatingAttributes.GEN_AI_SYSTEM, model.genAiSystem())
                         .setAttribute(GenAiIncubatingAttributes.GEN_AI_OPERATION_NAME, model.genAiOperation())
                         .setAttribute(GenAiIncubatingAttributes.GEN_AI_REQUEST_MODEL, model.genAiRequestModel())
                         .startSpan();
            System.out.println("started span");
            System.out.println(span);
        } else {
            span = null;
        }

        var wrapped = ActionListener.runBefore(listener, () -> {
            System.out.println("ending span");
            System.out.println(span);
            if (span != null) {
                span.end();
            }
        });

        try {
            requestSender.send(logger, request, hasFinished, responseHandler, wrapped);
        } catch (Exception e) {
            var errorMessage = Strings.format("Failed to send request from inference entity id [%s]", inferenceEntityId);
            logger.warn(errorMessage, e);
            wrapped.onFailure(new ElasticsearchException(errorMessage, e));
        }
    }

    public RequestSender requestSender() {return requestSender;}

    public Logger logger() {return logger;}

    public Request request() {return request;}

    public ResponseHandler responseHandler() {return responseHandler;}

    public Supplier<Boolean> hasFinished() {return hasFinished;}

    public ActionListener<InferenceServiceResults> listener() {return listener;}

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {return true;}
        if (obj == null || obj.getClass() != this.getClass()) {return false;}
        var that = (ExecutableInferenceRequest) obj;
        return Objects.equals(this.requestSender, that.requestSender) &&
               Objects.equals(this.logger, that.logger) &&
               Objects.equals(this.request, that.request) &&
               Objects.equals(this.responseHandler, that.responseHandler) &&
               Objects.equals(this.hasFinished, that.hasFinished) &&
               Objects.equals(this.listener, that.listener);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requestSender, logger, request, responseHandler, hasFinished, listener);
    }

    @Override
    public String toString() {
        return "ExecutableInferenceRequest[" +
               "requestSender=" + requestSender + ", " +
               "logger=" + logger + ", " +
               "request=" + request + ", " +
               "responseHandler=" + responseHandler + ", " +
               "hasFinished=" + hasFinished + ", " +
               "listener=" + listener + ']';
    }

}
