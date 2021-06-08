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
using System.Threading.Tasks;

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
        public ActionResult MainPage()
        {
            string id = HttpContext.Session.GetString("id");

            if (HttpContext.Session.Id == id)
            {
                return View("MainPage", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }

        }

        // loads the DatabaseConnection View
        [HttpGet]
        public ActionResult DatabaseConnection()
        {
            return View("DatabaseConnection", "Home");
        }

        // loads the data_masking View
        [HttpGet]
        public ActionResult data_masking()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("data_masking", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the data_unification View
        [HttpGet]
        public ActionResult data_unification()
        {

            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("data_unification", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the remove_duplicates View
        [HttpGet]
        public ActionResult remove_duplicates()
        {

            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("remove_duplicates", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the restrictionss View
        [HttpGet]
        public ActionResult restrictions()
        {

            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("restrictions", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the missing_values View
        [HttpGet]
        public ActionResult missing_values()
        {

            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("missing_values", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the improve_Metatable daos[id].tabledatatypes View
        [HttpGet]
        public ActionResult improve_datatypes()
        {

            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("improve_datatypes", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the primary_keys View
        [HttpGet]
        public ActionResult primary_keys()
        {

            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("primary_keys", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the foreign_keys View
        [HttpGet]
        public ActionResult foreign_keys()
        {

            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("foreign_keys", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the table_defragmentation View
        [HttpGet]
        public ActionResult table_defragmentation()
        {

            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("table_defragmentation", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // loads the improve_indexes View
        [HttpGet]
        public ActionResult improve_indexes()
        {

            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("improve_indexes", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpGet]
        public ActionResult create_masks()
        {

            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("create_masks", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }
        [HttpGet]
        public ActionResult create_restrictions()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("create_restrictions", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpGet]
        public ActionResult Performance()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].updatePerformance();
                return View("Performance", daos[id].perf);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpGet]
        public ActionResult Selection()
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                return View("Selection", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        [HttpGet]
        public ActionResult Help()
        {
            return View("Help");
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        [HttpGet]
        public ActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        [HttpPost]
        public async Task<ActionResult> Connect(string connectionString)
        {
            // this method checks the connection string to see it is not empty
            if (connectionString == null || connectionString == "")
            {
                return Help();
            }
            return await ConnectTask(connectionString);
        }
        public async Task<ActionResult> ConnectTask(string connectionString)
        {
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
                daos[HttpContext.Session.Id].initMetatable();
                resetInfo();

                return await Task.Run<ActionResult>(() =>
                {
                    return RedirectToAction("MainPage");
                });
            }
            catch (Exception e)
            {
                // if it is not valid it return the Help View
                return await Task.Run<ActionResult>(() =>
                {
                    return RedirectToAction("Help");
                });
            }
        }

        // this method is used to go to the Selection page while saving the functionality selected and the table and column data
        [HttpPost]
        public ActionResult GoToSelection(string functionalitySelected)
        {
            string id = HttpContext.Session.GetString("id");
            resetInfo();
            daos[id].info.Functionality = functionalitySelected;
            daos[id].info.Functionalities_text = daos[id].tabledata.Functionalities_text[functionalitySelected];
            daos[id].info.Functionalities_need_columns = daos[id].tabledata.Functionalities_need_columns[functionalitySelected];
            daos[id].info.ColumnsSelected = daos[id].getColumns(false);
            if (HttpContext.Session.Id == id)
            {
                return RedirectToAction("Selection");
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }

        }

        // this method is used to go to the selected page while sending the corresponding functionality name and the selected columns and tables
        [HttpPost]
        public ActionResult GoToPage(string functionalitySelected, string selection)
        {
            string id = HttpContext.Session.GetString("id");

            if (HttpContext.Session.Id == id)
            {
                if (functionalitySelected != "MainPage")
                {
                    resetInfo(functionalitySelected);
                    if (selection.Contains(','))
                    {
                        daos[id].info.ColumnsSelected = daos[id].parseColumnSelection(selection);
                    }
                    else
                    {
                        daos[id].info.TablesSelected = daos[id].parseTableSelection(selection);
                    }
                    switch (functionalitySelected)
                    {
                        case "create_masks":
                            daos[id].getAvailableMasks();
                            break;
                        case "create_restrictions":
                            daos[id].info.ColumnsSelected = daos[id].getColumns(false);
                            break;
                        case "primary_keys":
                            daos[id].getPks();
                            break;
                        case "foreign_keys":
                            daos[id].getFks();
                            break;
                        case "remove_duplicates":
                            daos[id].getDuplicates();
                            break;
                        case "missing_values":
                            daos[id].findMissingValues();
                            break;
                        case "improve_indexes":
                            daos[id].getIndexes();
                            break;
                        case "improve_datatypes":
                            daos[id].getDatatypes();
                            break;
                        case "data_unification":
                            daos[id].getUnification();
                            break;
                        default:
                            // some functionalities do not need preparation
                            break;
                    }
                }
                else
                {
                    resetInfo();
                }
                return RedirectToAction(functionalitySelected, daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }

        }

        // this method is used to go to the selected page while sending the corresponding functionality name and the selected columns and tables
        [HttpPost]
        public ActionResult GoBackToPage(string functionalitySelected)
        {
            string id = HttpContext.Session.GetString("id");

            if (HttpContext.Session.Id == id)
            {
                if (functionalitySelected != "MainPage" && functionalitySelected != "Performance")
                {
                    if (functionalitySelected != "create_restrictions")
                    {
                        resetInfo(functionalitySelected);
                    }
                }
                else
                {
                    resetInfo();
                }
                return RedirectToAction(functionalitySelected, daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // this method is like the one above but when every column and table is selected
        [HttpPost]
        public ActionResult GoToPageAll(string functionalitySelected)
        {
            string id = HttpContext.Session.GetString("id");
            resetInfo();
            daos[id].info.Functionality = functionalitySelected;
            daos[id].info.Functionalities_text = daos[id].tabledata.Functionalities_text[functionalitySelected];
            daos[id].info.Functionalities_need_columns = daos[id].tabledata.Functionalities_need_columns[functionalitySelected];
            if (daos[id].tabledata.Functionalities_need_columns[functionalitySelected])
            {
                daos[id].info.ColumnsSelected = daos[id].getColumns(false);
            }
            else
            {
                daos[id].info.TablesSelected = daos[id].tabledata.Tables;
            }

            if (HttpContext.Session.Id == id)
            {
                switch (functionalitySelected)
                {
                    case "create_masks":
                        daos[id].getAvailableMasks();
                        break;
                    case "create_restrictions":
                        daos[id].info.ColumnsSelected = daos[id].getColumns(false);
                        break;
                    case "primary_keys":
                        daos[id].getPks();
                        break;
                    case "foreign_keys":
                        daos[id].getFks();
                        break;
                    case "remove_duplicates":
                        daos[id].getDuplicates();
                        break;
                    case "missing_values":
                        daos[id].findMissingValues();
                        break;
                    case "improve_indexes":
                        daos[id].getIndexes();
                        break;
                    case "improve_datatypes":
                        daos[id].getDatatypes();
                        break;
                    case "data_unification":
                        daos[id].getUnification();
                        break;
                    default:
                        // some functionalities do not need preparation
                        break;
                }
                return RedirectToAction(functionalitySelected, daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // this method is only used for the 2 functionalities with extra steps to redirect after the create step
        [HttpPost]
        public ActionResult GoToPageAfterCreate(string functionalitySelected, string data)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                if (functionalitySelected == "data_masking")
                {
                    saveMaskTypes(data, true);
                    daos[id].getPrimaryKeysRecords();
                }
                else
                {
                    daos[id].getRestrictions();
                }
                return RedirectToAction(functionalitySelected, daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // this method is only used to confirm the changes to the database
        [HttpPost]
        public ActionResult Confirm(string data, string functionalitySelected)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].update(data);
                daos[id].tabledata.Log.Add(functionalitySelected + "\t" + DateTime.Now.ToString());
                return RedirectToAction("MainPage", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // this method is used to get the records of a specific column
        [HttpPost]
        public ActionResult GetRecord(string record, string functionalitySelected, string accordionInfo, string data2)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].info.TableAccordion = accordionInfo;
                daos[id].info.ColumnAccordion = record;

                if (data2 != "undefined")
                {
                    saveMaskTypes(data2, false);
                }
                daos[id].getRecord(record);
                return View(functionalitySelected, daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // this method is used to specify the restrictions in the restrictions functionality
        [HttpPost]
        public ActionResult ManageRestrictions(string table, string index, string column1, string column2)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].info.TableAccordion = table;

                if (column2 == null && column1 == null)
                {
                    Restriction r = daos[id].info.restrictions.Where(r => r.table == table).ToArray()[Int32.Parse(index)];
                    daos[id].info.restrictions.Remove(r);
                }
                else
                {
                    if (!daos[id].info.restrictions.Contains(new Restriction(table, column1, column2)))
                    {
                        daos[id].info.restrictions.Add(new Restriction(table, column1, column2));
                    }
                }
                return View("create_restrictions", daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // this method is used to get the records of a specific column
        [HttpPost]
        public ActionResult GetMissingValue(string missingValue, string functionalitySelected)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                daos[id].info.ColumnAccordion = missingValue;
                daos[id].getMissingValue(missingValue);
                return View(functionalitySelected, daos[id].info);
            }
            else
            {
                return RedirectToAction("DatabaseConnection");
            }
        }

        // This method saves the masks selected masks from the dropdowns in the corresponding Model variable
        public void saveMaskTypes(string data, bool deleteSelection)
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
            daos[id].info.MasksSelected = types;
            if (deleteSelection)
            {
                daos[id].info.ColumnsSelected = selection;
                daos[id].getMaskedRecords();
            }

        }

        // This method is used to send the simplest model possible to the view to improve performance
        public void resetInfo()
        {
            string id = HttpContext.Session.GetString("id");
            string database = daos[id].tabledata.Database;
            daos[id].info = new Interchange(database);
            daos[id].info.Records = new Dictionary<string, string[]>();
            daos[id].info.TablePks = new Dictionary<string, string[]>();
            daos[id].info.restrictions = new List<Restriction>();
        }

        // This method is used to send the simplest model possible to the view to improve performance
        public void resetInfo(string functionality)
        {
            string id = HttpContext.Session.GetString("id");
            string[] tablesSelected = new string[0];
            Dictionary<string, string[]> columnsSelected = new Dictionary<string, string[]>();

            if (daos[id].tabledata.Functionalities_need_columns[functionality])
            {
                columnsSelected = daos[id].info.ColumnsSelected;
            }
            else
            {
                tablesSelected = daos[id].info.TablesSelected;
            }

            resetInfo();
            daos[id].info.Functionality = functionality;
            daos[id].info.Functionalities_text = daos[id].tabledata.Functionalities_text[functionality];
            daos[id].info.Functionalities_need_columns = daos[id].tabledata.Functionalities_need_columns[functionality];

            if (daos[id].tabledata.Functionalities_need_columns[functionality])
            {
                daos[id].info.ColumnsSelected = columnsSelected;
            }
            else
            {
                daos[id].info.TablesSelected = tablesSelected;
            }
        }
    }
}