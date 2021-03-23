using System;
using System.Collections.Generic;

namespace TFG.Models
{
    public class Metatable
    {
        public Metatable() { }

        public Metatable(string database)
        {
            this.database = database;
            this.masksAvailable = new Dictionary<string, bool[]>();
            this.functionalities_text = new Dictionary<string, string>();
            this.functionalities_need_columns = new Dictionary<string, bool>();

            // Datacleaning functionalities
            functionalities_text.Add("create_masks", "Data Masking");
            functionalities_need_columns.Add("create_masks", true);
            functionalities_text.Add("data_unification", "Data Unification");
            functionalities_need_columns.Add("data_unification", true);
            functionalities_text.Add("remove_duplicates", "Remove Duplicates");
            functionalities_need_columns.Add("remove_duplicates", true);
            functionalities_text.Add("create_constraints", "Constraints");
            functionalities_need_columns.Add("create_constraints", false);
            functionalities_text.Add("missing_values", "Treating Missing Values");
            functionalities_need_columns.Add("missing_values", true);

            // Tuning functionalities
            functionalities_text.Add("improve_datatypes", "Improve Datatypes");
            functionalities_need_columns.Add("improve_datatypes", true);
            functionalities_text.Add("primary_keys", "Add/Improve Primary Keys");
            functionalities_need_columns.Add("primary_keys", false);
            functionalities_text.Add("foreign_keys", "Add/Improve Foreign Keys");
            functionalities_need_columns.Add("foreign_keys", false);
            functionalities_text.Add("entry.Value_defragmentation", "Table Defragmentation");
            functionalities_need_columns.Add("entry.Value_defragmentation", false);
            functionalities_text.Add("improve_indexes", "Index Cleaning & Generation");
            functionalities_need_columns.Add("improve_indexes", false);
        }

        public string database { get; set; }
        public string functionality { get; set; }
        public string log { get; set; }
        public string tableAccordion { get; set; }
        public string columnAccordion { get; set; }
        public string[] TablesSelected { get; set; }
        public Dictionary<string, bool[]> masksAvailable { get; set; }
        public Dictionary<string, string[]> tablePks { get; set; }
        public Dictionary<string, string[]> tableSuggestedPks { get; set; }
        public Dictionary<string, string[]> ColumnsSelected { get; set; }
        public Dictionary<string, string> types { get; set; }
        public Dictionary<string, string[]> records { get; set; }

        // Creating some structures to ensure code reusability and prevent code duplication and if it's run on columns (true) or entry.Values (false)
        public Dictionary<string, string> functionalities_text { get; set; }
        public Dictionary<string, bool> functionalities_need_columns { get; set; }

    }
}
