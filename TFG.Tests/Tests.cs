using Microsoft.AspNetCore.Http;
using NUnit.Framework;
using System;
using Microsoft.Data.SqlClient;
using TFG.Controllers;

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
            try
            {
                SqlConnection con = new SqlConnection("server=localhost;database=master;Trusted_Connection=True;");
                con.Open();
                Assert.AreEqual(con.State, System.Data.ConnectionState.Open);
            } catch (Exception e)
            {
                Assert.Fail(e.GetType().Name);   
            }
            Assert.Pass();
        }

        [Test]
        public void BadConnectTest()
        {
            try
            {
                SqlConnection con = new SqlConnection("HolaKAse");
                con.Open();
                Assert.Fail();
            }
            catch (Exception e)
            {
                Assert.Pass();
            }
        }
    }
}