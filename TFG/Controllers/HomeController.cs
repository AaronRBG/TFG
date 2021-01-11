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
            
        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        // loads the MainPage View saving the database name in the viewdata to be accesed later
        public IActionResult MainPage()
        {
            ViewData["database"] = HttpContext.Session.GetString("database");
            return View("MainPage", "Home");
        }

        // loads the DatabaseConnection View
        public IActionResult DatabaseConnection()
        {
            return View("DatabaseConnection", "Home");
        }

        // loads the Selection View saving the database name in the viewdata to be accesed later
        public IActionResult Selection()
        {
            SqlDataAdapter adp = new SqlDataAdapter();
            DataSet dsTables = new DataSet();
            DataSet dsColumns = new DataSet();

            // gets the connection String stored in the session and opens it

            ViewData["database"] = HttpContext.Session.GetString("database");
            SqlConnection con = new SqlConnection(HttpContext.Session.GetString("connectionString"));
            con.Open();

            // then it runs a query that returns all the tables names from that database
            // then processes the result to run a nested for loop which itself runs another query that returns all the column names of that table
            // when the for loop is finished we have a double string array which stores for each table its name and the names of all its columns, then saves this in the viewbag

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

        // loads the Help View saving the database name in the viewdata to be accesed later
        public IActionResult Help()
        {
            ViewData["database"] = HttpContext.Session.GetString("database");
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
            // this method checks the connection string to see it is not empty
            if (connectionString == null || connectionString == "")
            {
                return Help();
            }

            // then tries to open it to see if it is valid
            SqlConnection con = new SqlConnection(connectionString);
            con.Open();

            bool boo = (con.State == System.Data.ConnectionState.Open);
            if (boo)
            {
                // if it is valid it gets the database name
                string[] splits = connectionString.Split(';');
                foreach (string splitted in splits)
                {
                    if (splitted.Contains("database"))
                    {
                        splits = splitted.Split('=');
                        break;
                    }
                }

                // and saves both the database name and the connection String in the session for later access and changes the view
                HttpContext.Session.SetString("database", splits[1]);
                HttpContext.Session.SetString("connectionString", connectionString);
                con.Close();
                return MainPage();
            }
            else
            {
                // if it is not valid it return the Help View
                return Help();
            }
        }
        public IActionResult GoToSelection(string functionalitySelected)
        {
            // this method is used to go to the Selection page while sending the corresponding functionality
            ViewData["functionalitySelected"] = functionalitySelected;
            return Selection();
        }
    }
}





