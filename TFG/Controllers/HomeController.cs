using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using TFG.Models;
using Microsoft.Data.SqlClient;
using System.Data;
using Microsoft.AspNetCore.Http;

namespace TFG.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private SqlConnection con;
            
        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            return View("Index", "Home");
        }

        public IActionResult DatabaseConnection()
        {
            return View("DatabaseConnection", "Home");
        }

        public IActionResult Selection()
        {
            SqlDataAdapter adp = new SqlDataAdapter();
            DataSet dsTables = new DataSet();
            DataSet dsColumns = new DataSet();

            this.con = new SqlConnection(HttpContext.Session.GetString("connectionString"));
            this.con.Open();

            adp.SelectCommand = new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", con);
            adp.Fill(dsTables, "tables");
            DataTable dtTables = dsTables.Tables["tables"];
            DataTable dtColumns;
            DataRow rows;
            string[][] res = new string[dtTables.Rows.Count][];
            for (int i = 0; i < dtTables.Rows.Count; i++)
            {
                rows = dtTables.Rows[i];
                string com = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + rows[0] + "'";

                adp.SelectCommand = new SqlCommand(com, con);
                adp.Fill(dsColumns, "columns");
                dtColumns = dsColumns.Tables["columns"];

                res[i] = new string[dtColumns.Rows.Count + 1];
                res[i][0] = (string)rows[0];

                for (int j = 1; j < dtColumns.Rows.Count + 1; j++)
                {
                    rows = dtColumns.Rows[j - 1];
                    res[i][j] = (string)rows[0];
                }
                dsColumns.Reset();
            }
            ViewBag.tableData = res;
            return View("Selection", "Home");
        }

        public IActionResult Help()
        {
            return View("Help", "Home");
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        [HttpPost]
        public IActionResult Connect(string connectionString)
        {
            //string strcon;
            //Get connection string from web.config file
            //strcon = ConfigurationSettings.AppSettings["connectionString"];
            if (connectionString == null)
            {
                return Help();
            }
            HttpContext.Session.SetString("connectionString", connectionString);
            this.con = new SqlConnection(connectionString);
            this.con.Open();

            bool boo = (this.con.State == System.Data.ConnectionState.Open);
            if (boo)
            {
                this.con.Close();
                return View("Index", "Home");
            }
            else
            {
                return Help();
            }
        }
        public IActionResult GoToSelection(string functionalitySelected)
        {
            ViewData["functionalitySelected"] = functionalitySelected;
            return Selection();
        }
    }
}





