using Microsoft.AspNetCore.Http;
using NUnit.Framework;
using System;
using Microsoft.Data.SqlClient;
using TFG.Controllers;
using Microsoft.AspNetCore.Mvc;

namespace TFG.Tests
{
    [TestFixture]
    public class Tests
    {
        public HomeController h;
        public Microsoft.AspNetCore.Mvc.RedirectToActionResult resConnect;
        public Microsoft.AspNetCore.Mvc.ViewResult res;

        [SetUp]
        public void Setup()
        {
            h = new HomeController();
        }

        [Test]
        public void ConnectTest()
        {
            resConnect = h.Connect("server=localhost;database=master;Trusted_Connection=True;");
            Assert.AreEqual("MainPage", resConnect.ActionName);
        }

        [Test]
        public void BadConnectTest()
        {
            resConnect = h.Connect("BadConnectionString");
            Assert.AreEqual("Help", resConnect.ActionName);
        }

        [Test]
        public void DatabaseConnectionTest()
        {
            res = h.DatabaseConnection();
            Assert.AreEqual("DatabaseConnection", res.ViewName);
        }

        [Test]
        public void MainPageTest()
        {
            res = h.MainPage();
            Assert.AreEqual("MainPage", res.ViewName);
        }

        [Test]
        public void Data_maskingTest()
        {
            res = h.Data_masking();
            Assert.AreEqual("data_masking", res.ViewName);
        }

        [Test]
        public void Data_unificationTest()
        {
            res = h.Data_unification();
            Assert.AreEqual("data_unification", res.ViewName);
        }

        [Test]
        public void Remove_duplicatesTest()
        {
            res = h.Remove_duplicates();
            Assert.AreEqual("remove_duplicates", res.ViewName);
        }

        [Test]
        public void RestrictionsTest()
        {
            res = h.Restrictions();
            Assert.AreEqual("restrictions", res.ViewName);
        }

        [Test]
        public void Missing_valuesTest()
        {
            res = h.Missing_values();
            Assert.AreEqual("missing_values", res.ViewName);
        }

        [Test]
        public void Improve_datatypesTest()
        {
            res = h.Improve_datatypes();
            Assert.AreEqual("improve_datatypes", res.ViewName);
        }

        [Test]
        public void Primary_keysTest()
        {
            res = h.Primary_keys();
            Assert.AreEqual("primary_keys", res.ViewName);
        }

        [Test]
        public void Foreign_keysTest()
        {
            res = h.Foreign_keys();
            Assert.AreEqual("foreign_keys", res.ViewName);
        }

        [Test]
        public void Improve_indexesTest()
        {
            res = h.Improve_indexes();
            Assert.AreEqual("improve_indexes", res.ViewName);
        }

        [Test]
        public void Create_masksTest()
        {
            res = h.Create_masks();
            Assert.AreEqual("create_masks", res.ViewName);
        }

        [Test]
        public void Create_restrictionsTest()
        {
            res = h.Create_restrictions();
            Assert.AreEqual("create_restrictions", res.ViewName);
        }

        [Test]
        public void PerformanceTest()
        {
            res = h.Performance();
            Assert.AreEqual("Performance", res.ViewName);
        }

        [Test]
        public void SelectionTest()
        {
            res = h.Selection();
            Assert.AreEqual("Selection", res.ViewName);
        }

        [Test]
        public void HelpTest()
        {
            res = h.Help();
            Assert.AreEqual("Help", res.ViewName);
        }

        [Test]
        public void GetCombinationsTest()
        {
            string[] input = {"a", "b", "c"};
            string[] result = MetatableDao.GetCombinations(input);
            Assert.AreEqual(7, result.Length);
            Assert.AreEqual("a", result[0]);
            Assert.AreEqual("b", result[1]);
            Assert.AreEqual("c", result[2]);
            Assert.AreEqual("a,b", result[3]);
            Assert.AreEqual("a,c", result[4]);
            Assert.AreEqual("b,c", result[5]);
            Assert.AreEqual("a,b,c", result[6]);
        }

        [Test]
        public void GetCombinationsWithIDTest()
        {
            string[] input = { "a", "b", "cID" };
            string[] result = MetatableDao.GetCombinations(input);
            Assert.AreEqual(7, result.Length);
            Assert.AreEqual("cID", result[0]);
            Assert.AreEqual("a", result[1]);
            Assert.AreEqual("b", result[2]);
            Assert.AreEqual("a,cID", result[3]);
            Assert.AreEqual("b,cID", result[4]);
            Assert.AreEqual("a,b", result[5]);
            Assert.AreEqual("a,b,cID", result[6]);
        }

        [Test]
        public void IsSpacialTest()
        {
            Assert.AreEqual(true, MetatableDao.IsSpacial("hierarchyid"));
        }

        [Test]
        public void BadIsSpacialTest()
        {
            Assert.AreEqual(false, MetatableDao.IsSpacial("nvarchar(50)"));
        }

        [Test]
        public void IsDNITest()
        {
            Assert.AreEqual(true, MetatableDao.IsDNI("50505050A"));
        }

        [Test]
        public void BadIsDNITest()
        {
            Assert.AreEqual(false, MetatableDao.IsDNI("5"));
        }

        [Test]
        public void IsEmailTest()
        {
            Assert.AreEqual(true, MetatableDao.IsEmail("pepe@gmail.com"));
        }

        [Test]
        public void BadIsEmailTest()
        {
            Assert.AreEqual(false, MetatableDao.IsEmail("5"));
        }

        [Test]
        public void IsPhoneTest()
        {
            Assert.AreEqual(true, MetatableDao.IsPhone("5606504654"));
        }

        [Test]
        public void BadIsPhoneTest()
        {
            Assert.AreEqual(false, MetatableDao.IsPhone("5"));
        }

        [Test]
        public void IsCCNTest()
        {
            Assert.AreEqual(true, MetatableDao.IsCCN("4984846456146546"));
        }

        [Test]
        public void BadIsCCNTest()
        {
            Assert.AreEqual(false, MetatableDao.IsCCN("5"));
        }

        [Test]
        public void StringSimilarTest()
        {
            Assert.AreEqual(true, MetatableDao.StringSimilar("helloworld", "hellworld"));
        }

        [Test]
        public void BadStringSimilarTest()
        {
            Assert.AreEqual(false, MetatableDao.StringSimilar("helloworld","exampleText"));
        }

    }
}