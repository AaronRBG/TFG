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
        private static Dictionary<string, MetatableDao> Daos { get; set; }

        public HomeController()
        {
        }

        // loads the MainPage View saving the database name in the viewdata to be accesed later

        [HttpGet]
        public ViewResult MainPage()
        {
            if (HttpContext is null)
            {
                return View("MainPage");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");

                if (HttpContext.Session.Id == id)
                {
                    return View("MainPage", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }

        }

        // loads the DatabaseConnection View
        [HttpGet]
        public ViewResult DatabaseConnection()
        {
            return View("DatabaseConnection");
        }

        // loads the data_masking View
        [HttpGet]
        public ViewResult Data_masking()
        {
            if (HttpContext is null)
            {
                return View("data_masking");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    return View("data_masking", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }

        // loads the data_unification View
        [HttpGet]
        public ViewResult Data_unification()
        {
            if (HttpContext is null)
            {
                return View("data_unification");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    return View("data_unification", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }

        // loads the remove_duplicates View
        [HttpGet]
        public ViewResult Remove_duplicates()
        {
            if (HttpContext is null)
            {
                return View("remove_duplicates");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    return View("remove_duplicates", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }

        // loads the restrictionss View
        [HttpGet]
        public ViewResult Restrictions()
        {
            if (HttpContext is null)
            {
                return View("restrictions");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    return View("restrictions", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }

        // loads the missing_values View
        [HttpGet]
        public ViewResult Missing_values()
        {
            if (HttpContext is null)
            {
                return View("missing_values");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    return View("missing_values", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }

        // loads the improve_Metatable daos[id].tabledatatypes View
        [HttpGet]
        public ViewResult Improve_datatypes()
        {
            if (HttpContext is null)
            {
                return View("improve_datatypes");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    return View("improve_datatypes", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }

        // loads the primary_keys View
        [HttpGet]
        public ViewResult Primary_keys()
        {
            if (HttpContext is null)
            {
                return View("primary_keys");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    return View("primary_keys", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }

        // loads the foreign_keys View
        [HttpGet]
        public ViewResult Foreign_keys()
        {
            if (HttpContext is null)
            {
                return View("foreign_keys");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    return View("foreign_keys", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }

        // loads the improve_indexes View
        [HttpGet]
        public ViewResult Improve_indexes()
        {
            if (HttpContext is null)
            {
                return View("improve_indexes");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    return View("improve_indexes", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }

        [HttpGet]
        public ViewResult Create_masks()
        {
            if (HttpContext is null)
            {
                return View("create_masks");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    return View("create_masks", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }
        [HttpGet]
        public ViewResult Create_restrictions()
        {
            if (HttpContext is null)
            {
                return View("create_restrictions");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    return View("create_restrictions", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }

        [HttpGet]
        public ViewResult Performance()
        {
            if (HttpContext is null)
            {
                return View("Performance");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    Daos[id].UpdatePerformance();
                    return View("Performance", Daos[id].Perf);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }

        [HttpGet]
        public ViewResult Selection()
        {
            if (HttpContext is null)
            {
                return View("Selection");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (HttpContext.Session.Id == id)
                {
                    return View("Selection", Daos[id].Info);
                }
                else
                {
                    return DatabaseConnection();
                }
            }
        }

        [HttpGet]
        public ViewResult Help()
        {
            if (HttpContext is null)
            {
                return View("Help");
            }
            else
            {
                string id = HttpContext.Session.GetString("id");
                if (id == null)
                {
                    return View("Help", new Help());
                }
                else
                {
                    return View("Help", Daos[id].Help);
                }
            }
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        [HttpGet]
        public ViewResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        [HttpPost]
        public RedirectToActionResult Connect(string connectionString)
        {
            // this method checks the connection string to see it is not empty
            if (connectionString == null || connectionString == "")
            {
                return RedirectToAction("Help");
            }

            // then tries to open it to see if it is valid
            try
            {
                SqlConnection con = new SqlConnection(connectionString);
                con.Open();

                if (HttpContext is not null)
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

                    // Save a id variable in the session to stop it for resetting, then save the connection and create the dao
                    HttpContext.Session.SetString("id", HttpContext.Session.Id);
                    Daos = new Dictionary<string, MetatableDao>
                    {
                    { HttpContext.Session.Id, new MetatableDao(new Metatable(splits[1]), con) }
                    };
                    Daos[HttpContext.Session.Id].LoadScripts();
                    Daos[HttpContext.Session.Id].InitMetatable();
                    ResetInfo();
                    Daos[HttpContext.Session.Id].Help.connected = true;
                }
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
        public ActionResult GoToSelection(string functionalitySelected)
        {
            string id = HttpContext.Session.GetString("id");
            ResetInfo();
            Daos[id].Info.Functionality = functionalitySelected;
            Daos[id].Info.Functionalities_text = Daos[id].Tabledata.Functionalities_text[functionalitySelected];
            Daos[id].Info.Functionalities_need_columns = Daos[id].Tabledata.Functionalities_need_columns[functionalitySelected];
            Daos[id].Info.ColumnsSelected = Daos[id].GetColumns(false);
            if (HttpContext.Session.Id == id)
            {
                return RedirectToAction("Selection");
            }
            else
            {
                return DatabaseConnection();
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
                    ResetInfo(functionalitySelected);
                    if (selection.Contains(','))
                    {
                        Daos[id].Info.ColumnsSelected = Daos[id].ParseColumnSelection(selection);
                    }
                    else
                    {
                        Daos[id].Info.TablesSelected = Daos[id].ParseTableSelection(selection);
                    }
                    switch (functionalitySelected)
                    {
                        case "create_masks":
                            Daos[id].GetAvailableMasks();
                            break;
                        case "create_restrictions":
                            Daos[id].Info.ColumnsSelected = Daos[id].GetColumns(false);
                            break;
                        case "primary_keys":
                            Daos[id].GetPks();
                            break;
                        case "foreign_keys":
                            Daos[id].GetFks();
                            break;
                        case "remove_duplicates":
                            Daos[id].GetDuplicates();
                            break;
                        case "missing_values":
                            Daos[id].FindMissingValues();
                            break;
                        case "improve_indexes":
                            Daos[id].GetIndexes();
                            break;
                        case "improve_datatypes":
                            Daos[id].GetDatatypes();
                            break;
                        case "data_unification":
                            Daos[id].GetUnification();
                            break;
                        default:
                            // some functionalities do not need preparation
                            break;
                    }
                }
                else
                {
                    ResetInfo();
                }
                return RedirectToAction(functionalitySelected, Daos[id].Info);
            }
            else
            {
                return DatabaseConnection();
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
                        ResetInfo(functionalitySelected);
                    }
                }
                else
                {
                    ResetInfo();
                }
                return RedirectToAction(functionalitySelected, Daos[id].Info);
            }
            else
            {
                return DatabaseConnection();
            }
        }

        // this method is like the one above but when every column and table is selected
        [HttpPost]
        public ActionResult GoToPageAll(string functionalitySelected)
        {
            string id = HttpContext.Session.GetString("id");
            ResetInfo();
            Daos[id].Info.Functionality = functionalitySelected;
            Daos[id].Info.Functionalities_text = Daos[id].Tabledata.Functionalities_text[functionalitySelected];
            Daos[id].Info.Functionalities_need_columns = Daos[id].Tabledata.Functionalities_need_columns[functionalitySelected];
            if (Daos[id].Tabledata.Functionalities_need_columns[functionalitySelected])
            {
                Daos[id].Info.ColumnsSelected = Daos[id].GetColumns(false);
            }
            else
            {
                Daos[id].Info.TablesSelected = Daos[id].Tabledata.Tables;
            }

            if (HttpContext.Session.Id == id)
            {
                switch (functionalitySelected)
                {
                    case "create_masks":
                        Daos[id].GetAvailableMasks();
                        break;
                    case "create_restrictions":
                        Daos[id].Info.ColumnsSelected = Daos[id].GetColumns(false);
                        break;
                    case "primary_keys":
                        Daos[id].GetPks();
                        break;
                    case "foreign_keys":
                        Daos[id].GetFks();
                        break;
                    case "remove_duplicates":
                        Daos[id].GetDuplicates();
                        break;
                    case "missing_values":
                        Daos[id].FindMissingValues();
                        break;
                    case "improve_indexes":
                        Daos[id].GetIndexes();
                        break;
                    case "improve_datatypes":
                        Daos[id].GetDatatypes();
                        break;
                    case "data_unification":
                        Daos[id].GetUnification();
                        break;
                    case "table_defragmentation":
                        string aux = "undefined," + Daos[id].ArrayToString(Daos[id].Info.TablesSelected, false);
                        return Confirm(aux, functionalitySelected);
                    default:
                        // some functionalities do not need preparation
                        break;
                }
                return RedirectToAction(functionalitySelected, Daos[id].Info);
            }
            else
            {
                return DatabaseConnection();
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
                    SaveMaskTypes(data, true);
                    Daos[id].GetPrimaryKeysRecords();
                }
                else
                {
                    Daos[id].GetRestrictions();
                }
                return RedirectToAction(functionalitySelected, Daos[id].Info);
            }
            else
            {
                return DatabaseConnection();
            }
        }

        // this method is only used to confirm the changes to the database
        [HttpPost]
        public ActionResult Confirm(string data, string functionalitySelected)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                Daos[id].Update(data);
                Daos[id].Tabledata.Log.Add(functionalitySelected + "\t" + DateTime.Now.ToString());
                return base.RedirectToAction("MainPage", Daos[id].Info);
            }
            else
            {
                return DatabaseConnection();
            }
        }

        // this method is used to get the records of a specific column
        [HttpPost]
        public ActionResult GetRecord(string record, string functionalitySelected, string accordionInfo, string data2)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                Daos[id].Info.TableAccordion = accordionInfo;
                Daos[id].Info.ColumnAccordion = record;

                if (data2 != "undefined")
                {
                    SaveMaskTypes(data2, false);
                }
                Daos[id].GetRecord(record);
                return View(functionalitySelected, Daos[id].Info);
            }
            else
            {
                return DatabaseConnection();
            }
        }

        // this method is used to specify the restrictions in the restrictions functionality
        [HttpPost]
        public ActionResult ManageRestrictions(string table, string index, string column1, string column2)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                Daos[id].Info.TableAccordion = table;

                if (column2 == null && column1 == null)
                {
                    Restriction r = Daos[id].Info.Restrictions.Where(r => r.Table == table).ToArray()[Int32.Parse(index)];
                    Daos[id].Info.Restrictions.Remove(r);
                }
                else
                {
                    if (!Daos[id].Info.Restrictions.Contains(new Restriction(table, column1, column2)))
                    {
                        Daos[id].Info.Restrictions.Add(new Restriction(table, column1, column2));
                    }
                }
                return View("create_restrictions", Daos[id].Info);
            }
            else
            {
                return DatabaseConnection();
            }
        }

        // this method is used to get the records of a specific column
        [HttpPost]
        public ActionResult GetMissingValue(string missingValue, string functionalitySelected)
        {
            string id = HttpContext.Session.GetString("id");
            if (HttpContext.Session.Id == id)
            {
                Daos[id].Info.ColumnAccordion = missingValue;
                Daos[id].GetMissingValue(missingValue);
                return View(functionalitySelected, Daos[id].Info);
            }
            else
            {
                return DatabaseConnection();
            }
        }

        // This method saves the masks selected masks from the dropdowns in the corresponding Model variable
        public void SaveMaskTypes(string data, bool deleteSelection)
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
                        aux[^1] = pair[1];
                        selection[pair[0]] = aux;
                    }
                    else
                    {
                        string[] aux = { pair[1] };
                        selection.Add(pair[0], aux);
                    }
                }
            }
            Daos[id].Info.MasksSelected = types;
            if (deleteSelection)
            {
                Daos[id].Info.ColumnsSelected = selection;
                Daos[id].GetMaskedRecords();
            }

        }

        // This method is used to send the simplest model possible to the view to improve performance
        public void ResetInfo()
        {
            string id = HttpContext.Session.GetString("id");
            string database = Daos[id].Tabledata.Database;
            Daos[id].Info = new Interchange(database)
            {
                Records = new Dictionary<string, string[]>(),
                TablePks = new Dictionary<string, string[]>(),
                Restrictions = new List<Restriction>()
            };
        }

        // This method is used to send the simplest model possible to the view to improve performance
        public void ResetInfo(string functionality)
        {
            string id = HttpContext.Session.GetString("id");
            string[] tablesSelected = Array.Empty<string>();
            Dictionary<string, string[]> columnsSelected = new Dictionary<string, string[]>();

            if (Daos[id].Tabledata.Functionalities_need_columns[functionality])
            {
                columnsSelected = Daos[id].Info.ColumnsSelected;
            }
            else
            {
                tablesSelected = Daos[id].Info.TablesSelected;
            }

            ResetInfo();
            Daos[id].Info.Functionality = functionality;
            Daos[id].Info.Functionalities_text = Daos[id].Tabledata.Functionalities_text[functionality];
            Daos[id].Info.Functionalities_need_columns = Daos[id].Tabledata.Functionalities_need_columns[functionality];

            if (Daos[id].Tabledata.Functionalities_need_columns[functionality])
            {
                Daos[id].Info.ColumnsSelected = columnsSelected;
            }
            else
            {
                Daos[id].Info.TablesSelected = tablesSelected;
            }
        }
    }
}