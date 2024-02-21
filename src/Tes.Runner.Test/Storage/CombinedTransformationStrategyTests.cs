﻿// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Azure.Storage.Sas;
using Moq;
using Tes.Runner.Storage;

namespace Tes.Runner.Test.Storage
{
    [TestClass, TestCategory("Unit")]
    public class CombinedTransformationStrategyTests
    {
        private CombinedTransformationStrategy strategy = null!;
        private Mock<IUrlTransformationStrategy> mockStrategy1 = null!;
        private Mock<IUrlTransformationStrategy> mockStrategy2 = null!;

        [TestInitialize]
        public void SetUp()
        {
            mockStrategy1 = new Mock<IUrlTransformationStrategy>();
            mockStrategy2 = new Mock<IUrlTransformationStrategy>();

            strategy = new CombinedTransformationStrategy(
                new List<IUrlTransformationStrategy>()
                {
                    mockStrategy1.Object,
                    mockStrategy2.Object
                }
                );
        }

        [TestMethod]
        public async Task CombinedTransformationStrategy_TwoStrategiesProvided_BothAreAppliedInOrder()
        {
            var sourceUrl = "https://source.foo/";
            var firstTransformation = "https://first.foo/";
            var secondTransformation = "https://second.foo/";

            mockStrategy1.Setup(s => s.TransformUrlWithStrategyAsync(sourceUrl, It.IsAny<BlobSasPermissions>()))
                .ReturnsAsync(() => new Uri(firstTransformation));
            mockStrategy2.Setup(s => s.TransformUrlWithStrategyAsync(firstTransformation, It.IsAny<BlobSasPermissions>()))
                .ReturnsAsync(() => new Uri(secondTransformation));

            var result = await strategy.TransformUrlWithStrategyAsync(sourceUrl, new BlobSasPermissions());

            mockStrategy1.Verify(s => s.TransformUrlWithStrategyAsync(sourceUrl, It.IsAny<BlobSasPermissions>()), Times.Once);
            mockStrategy2.Verify(s => s.TransformUrlWithStrategyAsync(firstTransformation, It.IsAny<BlobSasPermissions>()), Times.Once);

            Assert.AreEqual(secondTransformation, result.ToString());
        }
    }
}
