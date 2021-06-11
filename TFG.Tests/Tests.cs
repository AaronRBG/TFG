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
        [SetUp]
        public void Setup()
        {
            // This is empty cause I say so warning
        }

        [Test]
        public void ConnectTest()
        {
            HomeController h = new HomeController();
            Microsoft.AspNetCore.Mvc.RedirectToActionResult res = h.Connect("server=localhost;database=master;Trusted_Connection=True;");
            Assert.AreEqual("MainPage", res.ActionName);
        }

        [Test]
        public void BadConnectTest()
        {
            HomeController h = new HomeController();
            Microsoft.AspNetCore.Mvc.RedirectToActionResult res = h.Connect("BadConnectionString");
            Assert.AreEqual("Help", res.ActionName);
        }

        [Test]
        public void DatabaseConnectionTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.DatabaseConnection();
            Assert.AreEqual("DatabaseConnection", res.ViewName);
        }

        [Test]
        public void MainPageTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.MainPage();
            Assert.AreEqual("MainPage", res.ViewName);
        }

        [Test]
        public void Data_maskingTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Data_masking();
            Assert.AreEqual("data_masking", res.ViewName);
        }

        [Test]
        public void Data_unificationTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Data_unification();
            Assert.AreEqual("data_unification", res.ViewName);
        }

        [Test]
        public void Remove_duplicatesTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Remove_duplicates();
            Assert.AreEqual("remove_duplicates", res.ViewName);
        }

        [Test]
        public void RestrictionsTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Restrictions();
            Assert.AreEqual("restrictions", res.ViewName);
        }

        [Test]
        public void Missing_valuesTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Missing_values();
            Assert.AreEqual("missing_values", res.ViewName);
        }

        [Test]
        public void Improve_datatypesTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Improve_datatypes();
            Assert.AreEqual("improve_datatypes", res.ViewName);
        }

        [Test]
        public void Primary_keysTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Primary_keys();
            Assert.AreEqual("primary_keys", res.ViewName);
        }

        [Test]
        public void Foreign_keysTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Foreign_keys();
            Assert.AreEqual("foreign_keys", res.ViewName);
        }

        [Test]
        public void Improve_indexesTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Improve_indexes();
            Assert.AreEqual("improve_indexes", res.ViewName);
        }

        [Test]
        public void Create_masksTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Create_masks();
            Assert.AreEqual("create_masks", res.ViewName);
        }

        [Test]
        public void Create_restrictionsTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Create_restrictions();
            Assert.AreEqual("create_restrictions", res.ViewName);
        }

        [Test]
        public void PerformanceTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Performance();
            Assert.AreEqual("Performance", res.ViewName);
        }

        [Test]
        public void SelectionTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Selection();
            Assert.AreEqual("Selection", res.ViewName);
        }

        [Test]
        public void HelpTest()
        {
            HomeController h = new HomeController();
            ViewResult res = h.Help();
            Assert.AreEqual("Help", res.ViewName);
        }

    }
}