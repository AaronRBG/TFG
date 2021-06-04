using System;
using System.Collections.Generic;

namespace TFG.Models
{
    public class Constraint
    {
        public Constraint() { }

        public Constraint(string name, string table, string col_type)
        {
            this.name = name;
            this.table = table;
            if (col_type == "FOREIGN KEY" || col_type == "PRIMARY KEY" || col_type == "INDEX")
            {
                this.type = col_type;
                if (col_type != "INDEX")
                {
                    this.column = name.Split('_')[name.Split('_').Length - 1];
                }
            }
            else
            {
                this.type = "COMPUTED COLUMN";
                this.column = col_type;
            }

        }

        public Constraint(string name, string table, string def_tab, string col_type) : this(name, table, col_type)
        {
            if (col_type == "FOREIGN KEY")
            {
                this.table2 = def_tab;
            }
            else if (col_type == "INDEX")
            {
                this.column = def_tab;
            }
            else
            {
                this.definition = def_tab;
            }
        }

        public string name { get; set; }
        public string table { get; set; }
        public string table2 { get; set; }
        public string column { get; set; }
        public string definition { get; set; }
        public string type { get; set; }

        public override bool Equals(Object obj)
        {
            return this.table == ((Constraint)obj).table && this.table2 == ((Constraint)obj).table2 && this.name == ((Constraint)obj).name && this.type == ((Constraint)obj).type && this.column == ((Constraint)obj).column;
        }

    }
}
