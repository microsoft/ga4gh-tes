// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System;
using System.Collections.Generic;
using Tes.Runner.Models;

namespace TesApi.Web
{
    public class NodeTaskBuilder
    {
        private readonly NodeTask nodeTask;

        public NodeTaskBuilder()
        {
            nodeTask = new NodeTask();
        }

        /// <summary>
        /// Sets the Id of the NodeTask
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithId(string id)
        {
            ArgumentException.ThrowIfNullOrEmpty(id, nameof(id));

            nodeTask.Id = id;
            return this;
        }

        /// <summary>
        /// Sets the workflow ID of the NodeTask
        /// </summary>
        /// <param name="workflowId"></param>
        /// <returns></returns>
        public NodeTaskBuilder WithWorkflowId(string workflowId)
        {
            ArgumentException.ThrowIfNullOrEmpty(workflowId, nameof(workflowId));

            nodeTask.WorkflowId = workflowId;
            return this;
        }

        public NodeTaskBuilder WithInput(string path, string sourceUrl, string mountParentDirectory)
        {
            ArgumentException.ThrowIfNullOrEmpty(path, nameof(path));
            ArgumentException.ThrowIfNullOrEmpty(sourceUrl, nameof(sourceUrl));

            nodeTask.Inputs ??= new List<FileInput>();

            nodeTask.Inputs.Add(
                new FileInput()
                {
                    MountParentDirectory = mountParentDirectory,
                    Path = path,
                    SourceUrl = sourceUrl,
                }
            );

            return this;
        }
    }
}
