using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using TFG.Models;
using Microsoft.Data.SqlClient;
using System.Data;
using Microsoft.AspNetCore.Http;
using System.Collections.Generic;
using System;

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
            try
            {
                return View("MainPage", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
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
            try
            {
                return View("data_masking", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the data_unification View
        [HttpGet]
        public IActionResult data_unification()
        {
            try
            {
                return View("data_unification", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the remove_duplicates View
        [HttpGet]
        public IActionResult remove_duplicates()
        {
            try
            {
                return View("remove_duplicates", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the constraints View
        [HttpGet]
        public IActionResult constraints()
        {
            try
            {
                return View("constraints", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the missing_values View
        [HttpGet]
        public IActionResult missing_values()
        {
            try
            {
                return View("missing_values", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the improve_datatypes View
        [HttpGet]
        public IActionResult improve_datatypes()
        {
            try
            {
                return View("improve_datatypes", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the primary_keys View
        [HttpGet]
        public IActionResult primary_keys()
        {
            try
            {
                return View("primary_keys", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the foreign_keys View
        [HttpGet]
        public IActionResult foreign_keys()
        {
            try
            {
                return View("foreign_keys", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the table_defragmentation View
        [HttpGet]
        public IActionResult table_defragmentation()
        {
            try
            {
                return View("table_defragmentation", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the improve_indexes View
        [HttpGet]
        public IActionResult improve_indexes()
        {
            try
            {
                return View("improve_indexes", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpGet]
        public IActionResult create_masks()
        {
            try
            {
                Manager.Instance().selections[HttpContext.Session.Id].records = new Dictionary<string, string[]>();
                return View("create_masks", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }
        [HttpGet]
        public IActionResult create_constraints()
        {
            try
            {
                return View("create_constraints", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpGet]
        public IActionResult Performance()
        {
            try
            {
                return View("Performance", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpGet]
        public IActionResult Selection()
        {
            try
            {
                return View("Selection", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

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

            if (con.State == System.Data.ConnectionState.Open)
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

                // Save a init variable in the session to stop it for resetting, then save the connection and the database name
                HttpContext.Session.SetInt32("init", 0);
                if (Manager.Instance().connections.ContainsKey(HttpContext.Session.Id))
                {
                    Manager.Instance().connections.Remove(HttpContext.Session.Id);
                    Manager.Instance().selections.Remove(HttpContext.Session.Id);
                }

                Manager.Instance().connections.Add(HttpContext.Session.Id, con);
                Manager.Instance().selections.Add(HttpContext.Session.Id, new ScriptsResults(splits[1], HttpContext.Session.Id));

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
            // this method is used to go to the Selection page while saving the functionality selected and the table and column data
            try
            {
                Manager.Instance().selections[HttpContext.Session.Id].functionality = functionalitySelected;
                Manager.Instance().selections[HttpContext.Session.Id].ColumnsSelected = Manager.Instance().getTableAndColumnData(HttpContext.Session.Id);
                return RedirectToAction("Selection");
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }
        [HttpPost]
        public IActionResult GoToPage(string functionalitySelected, string selection)
        {
            // this method is used to go to the selected page while sending the corresponding functionality name and the selected columns and tables
            try
            {
                Manager.Instance().saveSelections(selection, HttpContext.Session.Id);
                return RedirectToAction(functionalitySelected, Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpPost]
        public IActionResult GoBackToPage(string functionalitySelected)
        {
            // this method is used to go to the selected page while sending the corresponding functionality name and the selected columns and tables
            try
            {
                return RedirectToAction(functionalitySelected, Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpPost]
        public IActionResult GoToPageAll(string functionalitySelected)
        {
            // this method is like the one above but when every column and table is selected
            try
            {
                Manager.Instance().selections[HttpContext.Session.Id].functionality = functionalitySelected;
                Manager.Instance().selections[HttpContext.Session.Id].ColumnsSelected = Manager.Instance().getTableAndColumnData(HttpContext.Session.Id);
                return RedirectToAction(functionalitySelected, Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpPost]
        public IActionResult GoToPageAfterCreate(string functionalitySelected, string data)
        {
            // this method is only used for the 2 functionalities with extra steps to redirect after the create step
            try
            {
                Manager.Instance().saveTypes(HttpContext.Session.Id, data);
                return RedirectToAction(functionalitySelected, Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpPost]
        public IActionResult Confirm(string functionalitySelected)
        {
            // this method is only used to confirm the changes to the database
            try
            {
                Manager.Instance().update(HttpContext.Session.Id);
                Manager.Instance().selections[HttpContext.Session.Id].log += functionalitySelected + "\t" + DateTime.Now.ToString();
                return RedirectToAction("MainPage", Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpPost]
        public IActionResult GetRecord(string record, string functionalitySelected)
        {
            // this method is only used to confirm the changes to the database
            try
            {
                Manager.Instance().selections[HttpContext.Session.Id].records = Manager.Instance().getRecords(HttpContext.Session.Id, record);
                return View(functionalitySelected, Manager.Instance().selections[HttpContext.Session.Id]);
            }
            catch (KeyNotFoundException)
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

    }
}