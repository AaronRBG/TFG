using System;
using System.Collections.Generic;

namespace TFG.Models
{
    public class Metatable
    {
        public Metatable(){}

        public Metatable(string database)
        {
            this.database = database;
            this.masksAvailable = new Dictionary<string, bool[]>();
        }

        public string database { get; set; }
        public string functionality { get; set; }
        public string log { get; set; }
        public string tableAccordion { get; set; }
        public string columnAccordion { get; set; }
        public Dictionary<string, bool[]> masksAvailable { get; set; }
        public Dictionary<string, string[]> tablePks { get; set; }
        public Dictionary<string, string[]> ColumnsSelected { get; set; }
        public Dictionary<string, string> types { get; set; }
        public Dictionary<string, string[]> records { get; set; }
    }
}
