/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingClusterStateUpdateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettingProviders;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperService.MergeReason;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;

import java.io.IOException;

/**
 * Service responsible for submitting mapping changes
 */
public class MetadataMappingService {

    private static final Logger logger = LogManager.getLogger(MetadataMappingService.class);

    private final ClusterService clusterService;
    private final IndicesService indicesService;

    private final MasterServiceTaskQueue<PutMappingClusterStateUpdateTask> taskQueue;

    @Inject
    public MetadataMappingService(
        ClusterService clusterService,
        IndicesService indicesService,
        IndexSettingProviders indexSettingProviders
    ) {
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.taskQueue = clusterService.createTaskQueue("put-mapping", Priority.HIGH, new PutMappingExecutor(indexSettingProviders));
    }

    record PutMappingClusterStateUpdateTask(PutMappingClusterStateUpdateRequest request, ActionListener<AcknowledgedResponse> listener)
        implements
            ClusterStateTaskListener,
            ClusterStateAckListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public boolean mustAck(DiscoveryNode discoveryNode) {
            return true;
        }

        @Override
        public void onAllNodesAcked() {
            listener.onResponse(AcknowledgedResponse.of(true));
        }

        @Override
        public void onAckFailure(Exception e) {
            listener.onResponse(AcknowledgedResponse.of(false));
        }

        @Override
        public void onAckTimeout() {
            listener.onResponse(AcknowledgedResponse.FALSE);
        }

        @Override
        public TimeValue ackTimeout() {
            return request.ackTimeout();
        }
    }

    class PutMappingExecutor implements ClusterStateTaskExecutor<PutMappingClusterStateUpdateTask> {
        private final IndexSettingProviders indexSettingProviders;

        PutMappingExecutor() {
            this(IndexSettingProviders.EMPTY);
        }

        PutMappingExecutor(IndexSettingProviders indexSettingProviders) {
            this.indexSettingProviders = indexSettingProviders;
        }

        @Override
        public ClusterState execute(BatchExecutionContext<PutMappingClusterStateUpdateTask> batchExecutionContext) throws Exception {
            var currentState = batchExecutionContext.initialState();
            for (final var taskContext : batchExecutionContext.taskContexts()) {
                final var task = taskContext.getTask();
                final PutMappingClusterStateUpdateRequest request = task.request;
                try (var ignored = taskContext.captureResponseHeaders()) {
                    currentState = applyRequest(currentState, request);
                    taskContext.success(task);
                } catch (Exception e) {
                    taskContext.onFailure(e);
                }
            }
            return currentState;
        }

        private ClusterState applyRequest(ClusterState currentState, PutMappingClusterStateUpdateRequest request) throws IOException {
            final CompressedXContent mappingUpdateSource = request.source();
            final Metadata metadata = currentState.metadata();
            MergeReason reason = request.autoUpdate() ? MergeReason.MAPPING_AUTO_UPDATE : MergeReason.MAPPING_UPDATE;
            Metadata.Builder builder = Metadata.builder(metadata);
            boolean updated = false;
            // TODO: we should first group the indices by project
            for (Index index : request.indices()) {
                final ProjectMetadata projectMetadata = metadata.projectFor(index);
                final IndexMetadata indexMetadata = projectMetadata.index(index);
                final DocumentMapper updatedDocMapper;
                // do the actual merge here on the master, and update the mapping source
                try (MapperService mapperService = indicesService.createIndexMapperServiceForValidation(indexMetadata)) {
                    // add mappings for all types, we need them for cross-type validation
                    mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);

                    CompressedXContent existingSource = mapperService.documentMapper() != null
                        ? mapperService.documentMapper().mappingSource()
                        : null;
                    DocumentMapper mergedMapper = mapperService.merge(MapperService.SINGLE_MAPPING_NAME, mappingUpdateSource, reason);
                    CompressedXContent updatedSource = mergedMapper.mappingSource();
                    if (updatedSource.equals(existingSource)) {
                        continue;
                    }

                    logMappingResult(index, existingSource, updatedSource, mergedMapper);
                    // Mapping updates on a single type may have side-effects on other types so we need to
                    // update mapping metadata on all types
                    updatedDocMapper = mapperService.documentMapper();
                }

                IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexMetadata);
                if (updatedDocMapper != null) {
                    indexMetadataBuilder.putMapping(new MappingMetadata(updatedDocMapper));
                    indexMetadataBuilder.putInferenceFields(updatedDocMapper.mappers().inferenceFields());
                }
                boolean updatedSettings = false;
                final Settings.Builder additionalIndexSettings = Settings.builder();
                indexMetadataBuilder.mappingVersion(1 + indexMetadataBuilder.mappingVersion())
                    .mappingsUpdatedVersion(IndexVersion.current());
                for (IndexSettingProvider provider : indexSettingProviders.getIndexSettingProviders()) {
                    Settings.Builder newAdditionalSettingsBuilder = Settings.builder();
                    provider.onUpdateMappings(indexMetadata, updatedDocMapper, newAdditionalSettingsBuilder);
                    if (newAdditionalSettingsBuilder.keys().isEmpty() == false) {
                        Settings newAdditionalSettings = newAdditionalSettingsBuilder.build();
                        MetadataCreateIndexService.validateAdditionalSettings(provider, newAdditionalSettings, additionalIndexSettings);
                        additionalIndexSettings.put(newAdditionalSettings);
                        updatedSettings = true;
                    }
                }
                if (updatedSettings) {
                    final Settings.Builder indexSettingsBuilder = Settings.builder();
                    indexSettingsBuilder.put(indexMetadata.getSettings());
                    indexSettingsBuilder.put(additionalIndexSettings.build());
                    indexMetadataBuilder.settings(indexSettingsBuilder.build());
                    indexMetadataBuilder.settingsVersion(1 + indexMetadata.getSettingsVersion());
                }
                /*
                 * This implicitly increments the index metadata version and builds the index metadata. This means that we need to have
                 * already incremented the mapping version if necessary. Therefore, the mapping version increment must remain before this
                 * statement.
                 */
                builder.getProject(projectMetadata.id()).put(indexMetadataBuilder);
                updated = true;
            }
            if (updated) {
                return ClusterState.builder(currentState).metadata(builder).build();
            } else {
                return currentState;
            }
        }

        private void logMappingResult(
            Index index,
            CompressedXContent existingSource,
            CompressedXContent updatedSource,
            DocumentMapper mergedMapper
        ) {
            if (existingSource != null) {
                if (existingSource.equals(updatedSource)) {
                    // same source, no changes, ignore it
                } else {
                    // use the merged mapping source
                    if (logger.isDebugEnabled()) {
                        logger.debug("{} update_mapping [{}] with source [{}]", index, mergedMapper.type(), updatedSource);
                    } else if (logger.isInfoEnabled()) {
                        logger.info("{} update_mapping [{}]", index, mergedMapper.type());
                    }
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("{} create_mapping with source [{}]", index, updatedSource);
                } else if (logger.isInfoEnabled()) {
                    logger.info("{} create_mapping", index);
                }
            }
        }

    }

    public void putMapping(final PutMappingClusterStateUpdateRequest request, final ActionListener<AcknowledgedResponse> listener) {
        final ClusterState state = clusterService.state();
        boolean noop = true;
        for (Index index : request.indices()) {
            var project = state.metadata().lookupProject(index);
            if (project.isEmpty()) {
                // this is a race condition where the project got deleted from under a mapping update task
                noop = false;
                break;
            }
            final IndexMetadata indexMetadata = project.get().index(index);
            if (indexMetadata == null) {
                // local store recovery sends a mapping update request during application of a cluster state on the data node which we might
                // receive here before the CS update that created the index has been applied on all nodes and thus the index isn't found in
                // the state yet, but will be visible to the CS update below
                noop = false;
                break;
            }
            final MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata == null) {
                noop = false;
                break;
            }
            try (MapperService mapperService = indicesService.createIndexMapperServiceForValidation(indexMetadata)) {
                mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
                DocumentMapper mergedMapper = mapperService.merge(
                    MapperService.SINGLE_MAPPING_NAME,
                    request.source(),
                    MergeReason.MAPPING_UPDATE
                );
                CompressedXContent updatedSource = mergedMapper.mappingSource();
                logger.info(updatedSource.toString());
                logger.info(mappingMetadata.source().toString());
                if (updatedSource.equals(mappingMetadata.source()) == false) {
                    logger.info("Mapping update required for index {}", index);
                    noop = false;
                    break;
                }
            } catch (Exception e) {
                noop = false;
                break;
            }
        }
        if (noop) {
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        taskQueue.submitTask(
            "put-mapping " + Strings.arrayToCommaDelimitedString(request.indices()),
            new PutMappingClusterStateUpdateTask(request, listener),
            request.masterNodeTimeout()
        );
    }
}
