using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using TFG.Models;
using Microsoft.Data.SqlClient;
using System.Configuration;

namespace TFG.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;

        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult DatabaseConnection()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        [HttpPost]
        public void connect(string connectionString)
        {
            //string strcon;
            SqlConnection con;
            //Get connection string from web.config file
            //strcon = ConfigurationSettings.AppSettings["connectionString"];
            con = new SqlConnection(connectionString);
            con.Open();

            bool boo = (con.State == System.Data.ConnectionState.Open);
            if (boo) { System.Console.Write("connected"); }
            con.Close();
        }
    }
}





