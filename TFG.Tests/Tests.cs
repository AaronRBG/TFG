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

    }
}