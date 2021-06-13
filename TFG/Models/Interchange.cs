using System;
using System.Collections.Generic;

namespace TFG.Models
{
    public class Interchange
    {
        public Interchange() { }

        public Interchange(string database)
        {
            this.Database = database;
        }

        public string Database { get; set; }
        public string Functionality { get; set; }
        public string TableAccordion { get; set; }
        public string ColumnAccordion { get; set; }
        public string[] TablesSelected { get; set; }
        public List<Restriction> Restrictions { get; set; }
        public Dictionary<string, bool[]> MasksAvailable { get; set; }
        public Dictionary<string, string[]> TablePks { get; set; }
        public Dictionary<string, string[]> TableSuggestedPks { get; set; }
        public Models.Constraint[] TableFks { get; set; }
        public Models.Constraint[] TableSuggestedFks { get; set; }
        public Dictionary<string, string[]> ColumnsSelected { get; set; }
        public Dictionary<string, string> MasksSelected { get; set; }
        public Dictionary<string, string[]> Records { get; set; }
        public Dictionary<string, string> ColumnsDatatypes { get; set; }
        public Dictionary<string, string> ColumnsSuggestedDatatypes { get; set; }

        // Creating some structures to ensure code reusability and prevent code duplication and if it's run on columns (true) or entry.Values (false)
        public string Functionalities_text { get; set; }
        public bool Functionalities_need_columns { get; set; }

    }
}
