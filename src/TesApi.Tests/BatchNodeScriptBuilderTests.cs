// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Microsoft.VisualStudio.TestTools.UnitTesting;
using TesApi.Web;

// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

namespace TesApi.Tests
{
    [TestClass, TestCategory("Unit")]
    public class BatchNodeScriptBuilderTests
    {
        private BatchNodeScriptBuilder builder;

        [TestInitialize]
        public void SetUp()
        {
            builder = new BatchNodeScriptBuilder();
        }

        [TestMethod()]
        public void Build_ScriptEndsWithEchoTaskComplete()
        {
            var result = builder.WithMetrics()
                 .WithExecuteRunner()
                 .Build();

            Assert.IsTrue(result.EndsWith("echo Task complete\n"));
        }
    }
}
