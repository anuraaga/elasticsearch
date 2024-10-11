/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.Model;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.ResponseHandler;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import io.opentelemetry.semconv.incubating.GenAiIncubatingAttributes;

record ExecutableInferenceRequest(
    RequestSender requestSender,
    Logger logger,
    Request request,
    ResponseHandler responseHandler,
    Supplier<Boolean> hasFinished,
    ActionListener<InferenceServiceResults> listener
) implements Runnable {

    private static final AtomicLong REQUEST_ID = new AtomicLong();

    @Override
    public void run() {
        var inferenceEntityId = request.getInferenceEntityId();

        String spanId = "inference-"+REQUEST_ID.incrementAndGet();
        Model model = request.model2();
        ThreadPool threadPool = requestSender.threadPool();

        if (model != null && threadPool != null) {
            Map<String, Object> attrs = Map.of(
                GenAiIncubatingAttributes.GEN_AI_SYSTEM.getKey(), model.genAiSystem(),
                GenAiIncubatingAttributes.GEN_AI_OPERATION_NAME.getKey(), model.genAiOperation(),
                GenAiIncubatingAttributes.GEN_AI_REQUEST_MODEL.getKey(), model.genAiRequestModel()
            );
            requestSender.tracer().startTrace(threadPool.getThreadContext(), () -> spanId, model.genAiOperation() + " " + model.genAiRequestModel(), attrs);
        }

        var wrapped = ActionListener.runBefore(listener, () -> {
            requestSender.tracer().stopTrace(() -> spanId);
        });

        try {
            requestSender.send(logger, request, hasFinished, responseHandler, wrapped);
        } catch (Exception e) {
            var errorMessage = Strings.format("Failed to send request from inference entity id [%s]", inferenceEntityId);
            logger.warn(errorMessage, e);
            wrapped.onFailure(new ElasticsearchException(errorMessage, e));
        }
    }
}
