using System;
using System.Collections.Generic;

namespace TFG.Models
{
    public class Interchange
    {
        public Interchange() { }

        public Interchange(string database, string log)
        {
            this.Database = database;
            this.Log = log;
            this.Functionalities_text = new Dictionary<string, string>();
            this.Functionalities_need_columns = new Dictionary<string, bool>();

            // Datacleaning Functionalities
            Functionalities_text.Add("create_masks", "Data Masking");
            Functionalities_need_columns.Add("create_masks", true);
            Functionalities_text.Add("data_unification", "Data Unification");
            Functionalities_need_columns.Add("data_unification", true);
            Functionalities_text.Add("remove_duplicates", "Remove Duplicates");
            Functionalities_need_columns.Add("remove_duplicates", true);
            Functionalities_text.Add("create_constraints", "Constraints");
            Functionalities_need_columns.Add("create_constraints", false);
            Functionalities_text.Add("missing_values", "Treating Missing Values");
            Functionalities_need_columns.Add("missing_values", true);

            // Tuning Functionalities
            Functionalities_text.Add("improve_datatypes", "Improve Datatypes");
            Functionalities_need_columns.Add("improve_datatypes", true);
            Functionalities_text.Add("primary_keys", "Add/Improve Primary Keys");
            Functionalities_need_columns.Add("primary_keys", false);
            Functionalities_text.Add("foreign_keys", "Add/Improve Foreign Keys");
            Functionalities_need_columns.Add("foreign_keys", false);
            Functionalities_text.Add("entry.Value_defragmentation", "Table Defragmentation");
            Functionalities_need_columns.Add("entry.Value_defragmentation", false);
            Functionalities_text.Add("improve_indexes", "Index Cleaning & Generation");
            Functionalities_need_columns.Add("improve_indexes", false);
        }

        public string Database { get; set; }
        public string Functionality { get; set; }
        public string Log { get; set; }
        public string TableAccordion { get; set; }
        public string ColumnAccordion { get; set; }
        public string[] TablesSelected { get; set; }
        public Dictionary<string, bool[]> MasksAvailable { get; set; }
        public Dictionary<string, string[]> TablePks { get; set; }
        public Dictionary<string, string[]> TableSuggestedPks { get; set; }
        public Dictionary<string, string[]> ColumnsSelected { get; set; }
        public Dictionary<string, string> MasksSelected { get; set; }
        public Dictionary<string, string[]> Records { get; set; }

        // Creating some structures to ensure code reusability and prevent code duplication and if it's run on columns (true) or entry.Values (false)
        public Dictionary<string, string> Functionalities_text { get; set; }
        public Dictionary<string, bool> Functionalities_need_columns { get; set; }

    }
}
