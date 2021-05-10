using System;
using System.Collections.Generic;

namespace TFG.Models
{
    public class Constraint
    {
        public Constraint() { }

        public Constraint(string name, string table)
        {
            this.name = name;
            this.table = table;
            this.column = name.Split('_')[name.Split('_').Length - 1];
            this.type = "PRIMARY KEY";
        }

        public Constraint(string name, string table, string table2)
        {
            this.name = name;
            this.table = table;
            this.table2 = table2;
            this.column = name.Split('_')[name.Split('_').Length - 1];
            this.type = "FOREIGN KEY";
        }

        public Constraint(string name, string table, string definition, string column)
        {
            this.name = name;
            this.table = table;
            this.definition = definition;
            this.column = column;
            this.type = "COMPUTED COLUMN";
        }

        public string name { get; set; }
        public string table { get; set; }
        public string table2 { get; set; }
        public string column { get; set; }
        public string definition { get; set; }
        public string type { get; set; }
    }
}
