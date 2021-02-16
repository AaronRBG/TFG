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
            return View("MainPage", Manager.Instance().selections[HttpContext.Session.Id]);
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
            return View("data_masking", Manager.Instance().selections[HttpContext.Session.Id]);
        }

        // loads the data_unification View
        [HttpGet]
        public IActionResult data_unification()
        {
            return View("data_unification", Manager.Instance().selections[HttpContext.Session.Id]);
        }

        // loads the remove_duplicates View
        [HttpGet]
        public IActionResult remove_duplicates()
        {
            return View("remove_duplicates", Manager.Instance().selections[HttpContext.Session.Id]);
        }

        // loads the constraints View
        [HttpGet]
        public IActionResult constraints()
        {
            return View("constraints", Manager.Instance().selections[HttpContext.Session.Id]);
        }

        // loads the missing_values View
        [HttpGet]
        public IActionResult missing_values()
        {
            return View("missing_values", Manager.Instance().selections[HttpContext.Session.Id]);
        }

        // loads the improve_datatypes View
        [HttpGet]
        public IActionResult improve_datatypes()
        {
            return View("improve_datatypes", Manager.Instance().selections[HttpContext.Session.Id]);
        }

        // loads the primary_keys View
        [HttpGet]
        public IActionResult primary_keys()
        {
            return View("primary_keys", Manager.Instance().selections[HttpContext.Session.Id]);
        }

        // loads the foreign_keys View
        [HttpGet]
        public IActionResult foreign_keys()
        {
            return View("foreign_keys", Manager.Instance().selections[HttpContext.Session.Id]);
        }

        // loads the table_defragmentation View
        [HttpGet]
        public IActionResult table_defragmentation()
        {
            return View("table_defragmentation", Manager.Instance().selections[HttpContext.Session.Id]);
        }

        // loads the improve_indexes View
        [HttpGet]
        public IActionResult improve_indexes()
        {
            return View("improve_indexes", Manager.Instance().selections[HttpContext.Session.Id]);
        }

        [HttpGet]
        public IActionResult create_masks()
        {
            Manager.Instance().selections[HttpContext.Session.Id].records = Manager.Instance().getRecords(HttpContext.Session.Id);
            return View("create_masks", Manager.Instance().selections[HttpContext.Session.Id]);
        }
        [HttpGet]
        public IActionResult create_constraints()
        {
            return View("create_constraints", Manager.Instance().selections[HttpContext.Session.Id]);
        }

        [HttpGet]
        public IActionResult Performance()
        {
            return View("Performance", Manager.Instance().selections[HttpContext.Session.Id]);
        }

        [HttpGet]
        public IActionResult Selection()
        {
            return View("Selection", Manager.Instance().selections[HttpContext.Session.Id]);
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
            Manager.Instance().selections[HttpContext.Session.Id].functionality = functionalitySelected;
            Manager.Instance().selections[HttpContext.Session.Id].ColumnsSelected = Manager.Instance().getTableAndColumnData(HttpContext.Session.Id);
            return RedirectToAction("Selection");
        }
        [HttpPost]
        public IActionResult GoToPage(string functionalitySelected, string selection)
        {
            // this method is used to go to the selected page while sending the corresponding functionality name and the selected columns and tables
            Manager.Instance().saveSelections(selection, HttpContext.Session.Id);
            return RedirectToAction(functionalitySelected, Manager.Instance().selections[HttpContext.Session.Id]);
        }

        [HttpPost]
        public IActionResult GoBackToPage(string functionalitySelected)
        {
            // this method is used to go to the selected page while sending the corresponding functionality name and the selected columns and tables
            return RedirectToAction(functionalitySelected, Manager.Instance().selections[HttpContext.Session.Id]);
        }

        [HttpPost]
        public IActionResult GoToPageAll(string functionalitySelected)
        {
            // this method is like the one above but when every column and table is selected
            Manager.Instance().selections[HttpContext.Session.Id].ColumnsSelected = Manager.Instance().getTableAndColumnData(HttpContext.Session.Id);
            return RedirectToAction(functionalitySelected, Manager.Instance().selections[HttpContext.Session.Id]);
        }

        [HttpPost]
        public IActionResult GoToPageAfterCreate(string functionalitySelected, string data)
        {
            // this method is only used for the 2 functionalities with extra steps to redirect after the create step
            Manager.Instance().saveTypes(HttpContext.Session.Id, data);
            return RedirectToAction(functionalitySelected, Manager.Instance().selections[HttpContext.Session.Id]);
        }

        [HttpPost]
        public IActionResult Confirm(string functionalitySelected)
        {
            // this method is only used to confirm the changes to the database
            Manager.Instance().update(HttpContext.Session.Id);
            Manager.Instance().selections[HttpContext.Session.Id].log += functionalitySelected + "\t" + DateTime.Now.ToString();
            return RedirectToAction("MainPage", Manager.Instance().selections[HttpContext.Session.Id]);
        }

    }
}