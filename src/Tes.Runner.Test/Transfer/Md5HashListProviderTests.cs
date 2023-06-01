using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tes.Runner.Transfer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Tes.Runner.Test;

namespace Tes.Runner.Transfer.Tests
{
    [TestClass]
    [TestCategory("Unit")]
    public class Md5HashListProviderTests
    {
        private Md5HashListProvider hashListProvider = new();

        [TestMethod]
        public void AddBlockHashTest_Md4HashesAreAddedToTheList()
        {
            
        }

        [TestMethod]
        public void GetRootHashTest()
        {
            Assert.Fail();
        }
    }
}