﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;

#nullable enable

namespace Tes.Models
{
    /// <summary>
    /// Contains task execution metrics when task is handled by Azure Batch
    /// </summary>
    public class BatchNodeMetrics
    {
        /// <summary>
        /// Executor docker image pull duration
        /// </summary>
        [TesTaskLogMetadataKey("executor_pull_duration_sec")]
        public List<double?>? ExecutorImagePullDurationInSeconds { get; set; }

        /// <summary>
        /// File download duration
        /// </summary>
        [TesTaskLogMetadataKey("download_duration_sec")]
        public double? FileDownloadDurationInSeconds { get; set; }

        /// <summary>
        /// Main command execution duration
        /// </summary>
        [TesTaskLogMetadataKey("executor_duration_sec")]
        public List<double?>? ExecutorDurationInSeconds { get; set; }

        /// <summary>
        /// File upload duration
        /// </summary>
        [TesTaskLogMetadataKey("upload_duration_sec")]
        public double? FileUploadDurationInSeconds { get; set; }

        /// <summary>
        /// Executor image size in GB
        /// </summary>
        [TesTaskLogMetadataKey("executor_image_size_gb")]
        public List<double?>? ExecutorImageSizeInGB { get; set; }

        /// <summary>
        /// File download size in GB
        /// </summary>
        [TesTaskLogMetadataKey("file_download_size_gb")]
        public double? FileDownloadSizeInGB { get; set; }

        /// <summary>
        /// File upload size in GB
        /// </summary>
        [TesTaskLogMetadataKey("file_upload_size_gb")]
        public double? FileUploadSizeInGB { get; set; }

        /// <summary>
        /// Disk space used in GB
        /// </summary>
        [TesTaskLogMetadataKey("disk_used_gb")]
        public double? DiskUsedInGB { get; set; }

        /// <summary>
        /// Max resident memory used in GB
        /// </summary>
        [TesTaskLogMetadataKey("max_res_mem_used_gb")]
        public double? MaxResidentMemoryUsedInGB { get; set; }

        /// <summary>
        /// Disk space used as percent of total
        /// </summary>
        [TesTaskLogMetadataKey("disk_used_pct")]
        public float? DiskUsedPercent { get; set; }

        /// <summary>
        /// Max resident memory used as percent of total
        /// </summary>
        [TesTaskLogMetadataKey("max_res_mem_used_pct")]
        public float? MaxResidentMemoryUsedPercent { get; set; }

        /// <summary>
        /// CPU Model Name
        /// </summary>
        [TesTaskLogMetadataKey("vm_cpu_model_name")]
        public string? VmCpuModelName { get; set; }
    }
}
