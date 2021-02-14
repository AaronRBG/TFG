using System;
using System.Collections.Generic;

namespace TFG.Models
{
    public class ScriptsResults
    {
        public ScriptsResults(string database, string functionality, Dictionary<string, string[]> columnsSelected)
        {
            this.database = database;
            this.functionality = functionality;
            this.ColumnsSelected = columnsSelected;
        }

        public ScriptsResults(string database, string functionality, string[] tablesSelected)
        {
            this.database = database;
            this.functionality = functionality;
            this.tablesSelected = tablesSelected;
        }

        public ScriptsResults(string database)
        {
            this.database = database;
        }

        public ScriptsResults(string database, string functionality)
        {
            this.database = database;
            this.functionality = functionality;
        }

        public string database { get; set; }
        public string functionality { get; set; }
        public string log { get; set; }
        public string[] tablesSelected { get; set; }
        public Dictionary<string, string[]> ColumnsSelected { get; set; }
        public Dictionary<string, string> types { get; set; }
        public Dictionary<string, string[]> records { get; set; }
    }
}
