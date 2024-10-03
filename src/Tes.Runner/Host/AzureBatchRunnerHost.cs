// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace Tes.Runner.Host
{
    internal class AzureBatchRunnerHost : RunnerHost
    {
        private const string NodeRootDir = "AZ_BATCH_NODE_ROOT_DIR";
        private const string NodeSharedDir = "AZ_BATCH_NODE_SHARED_DIR";
        private const string NodeTaskDir = "AZ_BATCH_TASK_DIR";
        private const string NodeTaskWorkDir = "AZ_BATCH_TASK_WORKING_DIR";

        public override string GetTaskWorkingContainerPath(FileInfo file)
        {
            var path = Path.GetRelativePath(Environment.GetEnvironmentVariable(NodeTaskWorkDir) ?? throw new InvalidOperationException("Task working directory not found"), file.FullName);

            if (path == file.FullName)
            {
                throw new InvalidOperationException("File is not in container.");
            }

            return $"/{path.TrimStart('/')}";
        }

        public override string GetTaskWorkingContainerPath(DirectoryInfo directory)
        {
            var path = Path.GetRelativePath(Environment.GetEnvironmentVariable(NodeTaskWorkDir) ?? throw new InvalidOperationException("Task working directory not found"), directory.FullName);

            if (path == directory.FullName)
            {
                throw new InvalidOperationException("File is not in container.");
            }

            return $"/{path.TrimStart('/')}";
        }

        public override DirectoryInfo GetTaskWorkingHostDirectory(string path)
        {
            return new(Path.Combine(Environment.GetEnvironmentVariable(NodeTaskWorkDir) ?? throw new InvalidOperationException("Task working directory not found"), path.TrimStart('/')));
        }

        public override FileInfo GetTaskWorkingHostFile(string path)
        {
            return new(Path.Combine(Environment.GetEnvironmentVariable(NodeTaskWorkDir) ?? throw new InvalidOperationException("Task working directory not found"), path.TrimStart('/')));
        }

        /// <inheritdoc/>
        public override FileInfo GetSharedFile(string path)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(path);

            if (Path.IsPathFullyQualified(path))
            {
                throw new ArgumentException($"'{nameof(path)}' must be a relative path.", nameof(path));
            }

            var sharedDir = Environment.GetEnvironmentVariable(NodeSharedDir) ?? throw new InvalidOperationException("Shared node directory is unknown.");
            var fullPath = Path.GetFullPath(Path.Combine(sharedDir, path));

            if (!fullPath.StartsWith(sharedDir + Path.DirectorySeparatorChar))
            {
                throw new ArgumentException($"'{nameof(path)}' must not escape the shared node directory.", nameof(path));
            }

            return new(fullPath);
        }

        public override Task NodeCleanupPreviousTasksAsync()
        {
            var rootDir = Environment.GetEnvironmentVariable(NodeRootDir) ?? throw new InvalidOperationException("Root node directory is unknown.");
            var taskDir = Environment.GetEnvironmentVariable(NodeTaskDir) ?? throw new InvalidOperationException("Task directory is unknown.");
            var taskRelativeDir = Path.GetRelativePath(rootDir, taskDir);

            if (Path.IsPathRooted(taskRelativeDir))
            {
                throw new InvalidOperationException("Task directory is not found in root node directory.");
            }

            var relativeDirParts = taskRelativeDir.Split(Path.DirectorySeparatorChar);
            var workitemsDir = Path.Combine(rootDir, relativeDirParts[0]);
            var jobRootDir = Path.Combine(workitemsDir, relativeDirParts[1]);

            _ = Parallel.ForEach(Directory.EnumerateDirectories(workitemsDir)
                    .Where(dir => !dir.StartsWith(jobRootDir))
                .Concat(Directory.EnumerateDirectories(jobRootDir)
                    .SelectMany(Directory.EnumerateDirectories)
                    .Where(dir => !dir.StartsWith(taskDir))),
                dir => Directory.Delete(dir, true));

            return Task.CompletedTask;
        }

        //public override void WriteMetric(string key, string value)
        //{
        //    throw new NotImplementedException();
        //}
    }
}
