using System;
using System.Collections.Generic;

namespace TFG.Models
{
    public class ScriptsResults
    {
        public ScriptsResults(){}

        public ScriptsResults(string database, string id)
        {
            this.database = database;
            this.id = id;
        }

        public string database { get; set; }
        public string functionality { get; set; }
        public string log { get; set; }
        public string id { get; set; }
        public string tableAccordion { get; set; }
        public string columnAccordion { get; set; }
        public string[] tablesSelected { get; set; }
        public Dictionary<string, string[]> ColumnsSelected { get; set; }
        public Dictionary<string, string> types { get; set; }
        public Dictionary<string, string[]> records { get; set; }
    }
}
