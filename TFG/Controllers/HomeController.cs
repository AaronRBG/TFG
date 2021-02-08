using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using TFG.Models;
using Microsoft.Data.SqlClient;
using System.Data;
using Microsoft.AspNetCore.Http;
using System.Collections.Generic;

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

        [HttpGet]
        public IActionResult MainPage()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"));
            return View("MainPage", model);
        }

        // loads the DatabaseConnection View
        [HttpGet]
        public IActionResult DatabaseConnection()
        {
            return View("DatabaseConnection", "Home");
        }
        // loads the data_masking View
        [HttpGet]
        public IActionResult data_masking()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"]);
            return View("data_masking", model);
        }

        // loads the data_unification View
        [HttpGet]
        public IActionResult data_unification()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"]);
            return View("data_unification", model);
        }

        // loads the remove_duplicates View
        [HttpGet]
        public IActionResult remove_duplicates()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"]);
            return View("remove_duplicates", model);
        }

        // loads the constraints View
        [HttpGet]
        public IActionResult constraints()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"]);
            return View("constraints", model);
        }

        // loads the missing_values View
        [HttpGet]
        public IActionResult missing_values()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"]);
            return View("missing_values", model);
        }

        // loads the improve_datatypes View
        [HttpGet]
        public IActionResult improve_datatypes()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"]);
            return View("improve_datatypes", model);
        }

        // loads the primary_keys View
        [HttpGet]
        public IActionResult primary_keys()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"]);
            return View("primary_keys", model);
        }

        // loads the foreign_keys View
        [HttpGet]
        public IActionResult foreign_keys()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"]);
            return View("foreign_keys", model);
        }

        // loads the table_defragmentation View
        [HttpGet]
        public IActionResult table_defragmentation()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"]);
            return View("table_defragmentation", model);
        }

        // loads the improve_indexes View
        [HttpGet]
        public IActionResult improve_indexes()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"]);
            return View("improve_indexes", model);
        }

        [HttpGet]
        public IActionResult create_masks()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"], getSelection());
            return View("create_masks", model);
        }
        [HttpGet]
        public IActionResult create_constraints()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"]);
            return View("create_constraints", model);
        }

        [HttpGet]
        public IActionResult Performance()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"));
            return View("Performance", model);
        }

        public string[][] getTableAndColumnData()
        {

            SqlDataAdapter adp = new SqlDataAdapter();
            DataSet dsTables = new DataSet();
            DataSet dsColumns = new DataSet();

            // gets the connection String stored in the session and opens it
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

            return res;
        }

        // loads the Selection View saving the database name in the viewdata to be accesed later
        [HttpGet]
        public IActionResult Selection()
        {
            ScriptsResults model = new ScriptsResults(HttpContext.Session.GetString("database"), (string)TempData["functionalitySelected"], getTableAndColumnData());
            return View("Selection", model);
        }

        // loads the Help View saving the database name in the viewdata to be accesed later
        [HttpGet]
        public IActionResult Help()
        {
            return View("Help");
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        [HttpGet]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        [HttpGet]
        public string[][] getSelection()
        {
            string selection = HttpContext.Session.GetString("selected");
            string[][] selected;
            if (selection != null && selection!="all")
            {
                string[] tables = selection.Split('/');
                selected = new string[tables.Length - 1][];
                for (int i = 1; i < tables.Length; i++)
                {
                    string[] columns = tables[i].Split(',');
                    selected[i - 1] = columns;
                }
            }
            else
            {
                selected = getTableAndColumnData();
            }
            return selected;
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
                ScriptsResults model = new ScriptsResults(splits[1]);
                return RedirectToAction("MainPage");
            }
            else
            {
                // if it is not valid it return the Help View
                return RedirectToAction("Help");
            }
        }
        [HttpPost]
        public IActionResult GoToSelection(string functionalitySelected)
        {
            // this method is used to go to the Selection page while sending the corresponding functionality
            TempData["functionalitySelected"] = functionalitySelected;
            return RedirectToAction("Selection");
        }
        [HttpPost]
        public IActionResult GoToPage(string functionalitySelected, string selection)
        {
            // this method is used to go to the Selection page while sending the corresponding functionality
            TempData["functionalitySelected"] = functionalitySelected;
            HttpContext.Session.SetString("selected", selection);
            return RedirectToAction(functionalitySelected, "Home");
        }

        [HttpPost]
        public IActionResult GoToPageAll(string functionalitySelected)
        {
            TempData["functionalitySelected"] = functionalitySelected;
            HttpContext.Session.SetString("selected", "all");
            return RedirectToAction(functionalitySelected, "Home");
        }

        [HttpPost]
        public IActionResult GoToPageAfterCreate(string functionalitySelected)
        {
            if (functionalitySelected == "create_masks")
            {
                TempData["functionalitySelected"] = "data_masking";
                return RedirectToAction("data_masking", "Home");
            }
            else
            {
                TempData["functionalitySelected"] = "constraints";
                return RedirectToAction("constraints", "Home");
            }

        }

    }
}