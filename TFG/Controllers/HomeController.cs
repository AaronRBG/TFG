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
using System.Linq;

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
                daos[HttpContext.Session.Id].loadScripts();

                return RedirectToAction("MainPage");
            }
            catch (Exception)
            {
                // if it is not valid it return the Help View
                return RedirectToAction("Help");
            }
        }

        // this method is used to go to the Selection page while saving the functionality selected and the table and column data
        [HttpPost]
        public IActionResult GoToSelection(string functionalitySelected)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].tabledata.functionality = functionalitySelected;
                daos[id].getTableAndColumnData();
                return RedirectToAction("Selection");
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // this method is used to go to the selected page while sending the corresponding functionality name and the selected columns and tables
        [HttpPost]
        public IActionResult GoToPage(string functionalitySelected, string selection)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                if (functionalitySelected != "MainPage")
                {
                    if (selection.Contains(','))
                    {
                        daos[id].tabledata.ColumnsSelected = parseColumnSelection(selection);
                    }
                    else
                    {
                        daos[id].tabledata.TablesSelected = parseTableSelection(selection);
                    }
                    if (functionalitySelected == "primary_keys")
                    {
                        daos[id].getSuggestedPks();
                    }
                }
                return RedirectToAction(functionalitySelected, daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // this method is used to go to the selected page while sending the corresponding functionality name and the selected columns and tables
        [HttpPost]
        public IActionResult GoBackToPage(string functionalitySelected)
        {
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

        // this method is like the one above but when every column and table is selected
        [HttpPost]
        public IActionResult GoToPageAll(string functionalitySelected)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].tabledata.functionality = functionalitySelected;
                if (daos[id].tabledata.functionalities_need_columns[functionalitySelected])
                {
                    daos[id].getTableAndColumnData();
                    daos[id].tabledata.TablesSelected = null;
                    daos[id].tabledata.records = null;
                }
                else
                {
                    daos[id].getTableData();
                    daos[id].tabledata.ColumnsSelected = null;
                    daos[id].tabledata.records = null;
                }
                if (functionalitySelected == "primary_keys")
                {
                    daos[id].getSuggestedPks();
                }
                return RedirectToAction(functionalitySelected, daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // this method is only used for the 2 functionalities with extra steps to redirect after the create step
        [HttpPost]
        public IActionResult GoToPageAfterCreate(string functionalitySelected, string data)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                saveTypes(data, true);
                return RedirectToAction(functionalitySelected, daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // this method is only used to confirm the changes to the database
        [HttpPost]
        public IActionResult Confirm(string data, string functionalitySelected)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                selectRows(data);
                daos[id].update();
                daos[id].tabledata.log += functionalitySelected + "\t" + DateTime.Now.ToString();
                return RedirectToAction("MainPage", daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // this method is only used to confirm the changes to the database
        [HttpPost]
        public IActionResult GetRecord(string record, string functionalitySelected, string accordionInfo, string data2)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].tabledata.tableAccordion = accordionInfo;
                daos[id].tabledata.columnAccordion = record;

                if (data2 != "undefined")
                {
                    saveTypes(data2, false);
                }
                daos[id].getRecord(record);
                return View(functionalitySelected, daos[id].tabledata);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // this method is only used to confirm the changes to the database
        [HttpPost]
        public IActionResult GetAvailableMasks(string name)
        {
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

        // This method saves the masks selected masks from the dropdowns in the corresponding Model variable
        public void saveTypes(string data, bool deleteSelection)
        {
            string id = HttpContext.Session.GetString("id");
            Dictionary<string, string> types = new Dictionary<string, string>();
            Dictionary<string, string[]> selection = new Dictionary<string, string[]>();

            string[] columns = data.Split('/');
            for (int i = 1; i < columns.Length; i++)
            {
                string[] names = columns[i].Split(',');
                types.Add(names[0], names[1]);

                string[] pair = names[0].Split('.');
                if (deleteSelection)
                {
                    if (selection.ContainsKey(pair[0]))
                    {
                        string[] aux = new string[selection[pair[0]].Length + 1];
                        for (int j = 0; j < selection[pair[0]].Length; j++)
                        {
                            aux[j] = selection[pair[0]][j];
                        }
                        aux[aux.Length - 1] = pair[1];
                        selection[pair[0]] = aux;
                    }
                    else
                    {
                        string[] aux = { pair[1] };
                        selection.Add(pair[0], aux);
                    }
                }
            }
            daos[id].tabledata.types = types;
            if (deleteSelection)
            {
                daos[id].tabledata.ColumnsSelected = selection;
            }
            daos[id].getMaskedRecords();
        }

        // This method saves the selection of the rows in the database that will be updated
        public void selectRows(string data)
        {
            Dictionary<string, string[]> res = parseColumnSelection(data);
            string id = HttpContext.Session.GetString("id");

            foreach (KeyValuePair<string, string[]> record in daos[id].tabledata.records)
            {
                foreach (KeyValuePair<string, string[]> entry in res)
                {
                    if (record.Key == entry.Key)
                    {
                        string[] aux = new string[entry.Value.Length];
                        string[] aux_masked = new string[entry.Value.Length];
                        int counter = 0;
                        for (int i = 0; i < record.Value.Length; i++)
                        {
                            if (entry.Value.Contains(i.ToString()))
                            {
                                aux[counter] = record.Value[i];
                                aux_masked[counter] = daos[id].tabledata.records[record.Key + "Masked"][i];
                                counter++;
                            }
                        }

                        daos[id].tabledata.records[record.Key] = aux;
                        daos[id].tabledata.records[record.Key + "Masked"] = aux_masked;
                    }
                }
            }
        }

        // This method parses a string into a dictionary
        private Dictionary<string, string[]> parseColumnSelection(string selection)
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            string[] tables = selection.Split('/');
            for (int i = 1; i < tables.Length; i++)
            {
                string[] columns = tables[i].Split(',');
                res.Add(columns[0], columns.Skip(1).ToArray());
            }

            return res;
        }

        // This method parses a string into an array
        private string[] parseTableSelection(string selection)
        {
            string[] tables = selection.Split('/');
            string[] res = new string[tables.Length - 1];
            Array.Copy(tables, 1, res, 0, res.Length);
            return res;
        }

    }
}