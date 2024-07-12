// Copyright (c) Microsoft Corporation.
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
            mockStrategy1 = new();
            mockStrategy2 = new();

            strategy = new(
                [
                    mockStrategy1.Object,
                    mockStrategy2.Object
                ]);
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

        [TestMethod]
        public async Task CombinedTransformationStrategy_CompactDrsUri()
        {
            var sourceUrl = "drs://my-uri:456_abc";
            var transformation = "https://first.foo/";

            mockStrategy1.Setup(s => s.TransformUrlWithStrategyAsync(sourceUrl, It.IsAny<BlobSasPermissions>()))
                .Throws(new UriFormatException());
            mockStrategy2.Setup(s => s.TransformUrlWithStrategyAsync(sourceUrl, It.IsAny<BlobSasPermissions>()))
                .ReturnsAsync(() => new Uri(transformation));

            var result = await strategy.TransformUrlWithStrategyAsync(sourceUrl, new BlobSasPermissions());

            mockStrategy1.Verify(s => s.TransformUrlWithStrategyAsync(sourceUrl, It.IsAny<BlobSasPermissions>()), Times.Once);
            mockStrategy2.Verify(s => s.TransformUrlWithStrategyAsync(sourceUrl, It.IsAny<BlobSasPermissions>()), Times.Once);

            Assert.AreEqual(transformation, result.ToString());
        }

        [TestMethod]
        public async Task CombinedTransformationStrategy_InsertsStrategyFirst_InsertedStrategyIsApplied()
        {
            var sourceUrl = "https://source.foo/";
            var firstTransformation = "https://first.foo/";
            var secondTransformation = "https://second.foo/";
            var thirdTransformation = "https://third.foo/";

            mockStrategy1.Setup(s => s.TransformUrlWithStrategyAsync(firstTransformation, It.IsAny<BlobSasPermissions>()))
                .ReturnsAsync(() => new Uri(secondTransformation));
            mockStrategy2.Setup(s => s.TransformUrlWithStrategyAsync(secondTransformation, It.IsAny<BlobSasPermissions>()))
                .ReturnsAsync(() => new Uri(thirdTransformation));

            var mockStrategy3 = new Mock<IUrlTransformationStrategy>();
            mockStrategy3.Setup(s => s.TransformUrlWithStrategyAsync(sourceUrl, It.IsAny<BlobSasPermissions>()))
                .ReturnsAsync(() => new Uri(firstTransformation));

            strategy.InsertStrategy(0, mockStrategy3.Object);

            var result = await strategy.TransformUrlWithStrategyAsync(sourceUrl, new BlobSasPermissions());

            mockStrategy3.Verify(s => s.TransformUrlWithStrategyAsync(sourceUrl, It.IsAny<BlobSasPermissions>()), Times.Once);
            mockStrategy1.Verify(s => s.TransformUrlWithStrategyAsync(firstTransformation, It.IsAny<BlobSasPermissions>()), Times.Once);
            mockStrategy2.Verify(s => s.TransformUrlWithStrategyAsync(secondTransformation, It.IsAny<BlobSasPermissions>()), Times.Once);

            Assert.AreEqual(thirdTransformation, result.ToString());
        }
    }
}
