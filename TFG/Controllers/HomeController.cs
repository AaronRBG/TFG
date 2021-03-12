using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using TFG.Models;
using Microsoft.Data.SqlClient;
using System.Data;
using Microsoft.AspNetCore.Http;
using System.Collections.Generic;
using System;
using System.Web;
using Microsoft.AspNetCore.Session;

namespace TFG.Controllers
{
    public class HomeController : Controller
    {

        private readonly ILogger<HomeController> _logger;
        private static Dictionary<string, MetatableDao> daos { get; set; }

        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        // loads the MainPage View saving the database name in the viewdata to be accesed later

        [HttpGet]
        public IActionResult MainPage()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("MainPage", daos[id].tabledata);
            }
            else
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
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("data_masking", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the data_unification View
        [HttpGet]
        public IActionResult data_unification()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("data_unification", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the remove_duplicates View
        [HttpGet]
        public IActionResult remove_duplicates()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("remove_duplicates", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the constraints View
        [HttpGet]
        public IActionResult constraints()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("constraints", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the missing_values View
        [HttpGet]
        public IActionResult missing_values()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("missing_values", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the improve_Metatable daos[id].tabledatatypes View
        [HttpGet]
        public IActionResult improve_datatypes()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("improve_datatypes", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the primary_keys View
        [HttpGet]
        public IActionResult primary_keys()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("primary_keys", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the foreign_keys View
        [HttpGet]
        public IActionResult foreign_keys()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("foreign_keys", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the table_defragmentation View
        [HttpGet]
        public IActionResult table_defragmentation()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("table_defragmentation", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the improve_indexes View
        [HttpGet]
        public IActionResult improve_indexes()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("improve_indexes", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpGet]
        public IActionResult create_masks()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].tabledata.records = new Dictionary<string, string[]>();
                return View("create_masks", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }
        [HttpGet]
        public IActionResult create_constraints()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("create_constraints", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpGet]
        public IActionResult Performance()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("Performance", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpGet]
        public IActionResult Selection()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("Selection", daos[id].tabledata);
            }
            else
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
            try
            {
                SqlConnection con = new SqlConnection(connectionString);
                con.Open();

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

                // Save a id variable in the session to stop it for resetting, then save the connection and create the dao
                HttpContext.Session.SetString("id", HttpContext.Session.Id);
                daos = new Dictionary<string, MetatableDao>();
                daos.Add(HttpContext.Session.Id, new MetatableDao(new Metatable(splits[1]), con));

                return RedirectToAction("MainPage");
            }
            catch (Exception)
            {
                // if it is not valid it return the Help View
                return RedirectToAction("Help");
            }
        }
        [HttpPost]
        public IActionResult GoToSelection(string functionalitySelected)
        {
            // this method is used to go to the Selection page while saving the functionality selected and the table and column data
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].tabledata.functionality = functionalitySelected;
                daos[id].tabledata.ColumnsSelected = daos[id].getTableAndColumnData();
                return RedirectToAction("Selection");
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }
        [HttpPost]
        public IActionResult GoToPage(string functionalitySelected, string selection)
        {
            // this method is used to go to the selected page while sending the corresponding functionality name and the selected columns and tables
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                if (functionalitySelected != "MainPage")
                {
                    daos[id].saveSelections(selection);
                }
                return RedirectToAction(functionalitySelected, daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpPost]
        public IActionResult GoBackToPage(string functionalitySelected)
        {
            // this method is used to go to the selected page while sending the corresponding functionality name and the selected columns and tables
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return RedirectToAction(functionalitySelected, daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpPost]
        public IActionResult GoToPageAll(string functionalitySelected)
        {
            // this method is like the one above but when every column and table is selected
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].tabledata.functionality = functionalitySelected;
                daos[id].tabledata.ColumnsSelected = daos[id].getTableAndColumnData();
                return RedirectToAction(functionalitySelected, daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpPost]
        public IActionResult GoToPageAfterCreate(string functionalitySelected, string data)
        {
            // this method is only used for the 2 functionalities with extra steps to redirect after the create step
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].saveTypes(data, true);
                return RedirectToAction(functionalitySelected, daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpPost]
        public IActionResult Confirm(string data, string functionalitySelected)
        {
            // this method is only used to confirm the changes to the database
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].selectRows(data);
                daos[id].update();
                daos[id].tabledata.log += functionalitySelected + "\t" + DateTime.Now.ToString();
                return RedirectToAction("MainPage", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpPost]
        public IActionResult GetRecord(string record, string functionalitySelected, string accordionInfo, string data2)
        {
            // this method is only used to confirm the changes to the database
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].tabledata.tableAccordion = accordionInfo;
                daos[id].tabledata.columnAccordion = record;

                if (data2 != "undefined")
                {
                    daos[id].saveTypes(data2, false);
                }
                daos[id].tabledata.records = daos[id].getRecords(record);
                return View(functionalitySelected, daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpPost]
        public IActionResult GetAvailableMasks(string name)
        {
            // this method is only used to confirm the changes to the database
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].getAvailableMasks(name);

                return View(daos[id].tabledata.functionality, daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

    }
}