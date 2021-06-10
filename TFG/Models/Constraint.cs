using System;
using System.Collections.Generic;

namespace TFG.Models
{
    public class Constraint
    {
        public Constraint() { }

        public Constraint(string name, string table, string col_type)
        {
            this.Name = name;
            this.Table = table;
            if (col_type == "FOREIGN KEY" || col_type == "PRIMARY KEY" || col_type == "INDEX")
            {
                this.Type = col_type;
                if (col_type != "INDEX")
                {
                    this.Column = name.Split('_')[^1];
                }
            }
            else
            {
                this.Type = "COMPUTED COLUMN";
                this.Column = col_type;
            }

        }

        public Constraint(string name, string table, string def_tab, string col_type) : this(name, table, col_type)
        {
            if (col_type == "FOREIGN KEY")
            {
                this.Table2 = def_tab;
            }
            else if (col_type == "INDEX")
            {
                this.Column = def_tab;
            }
            else
            {
                this.Definition = def_tab;
            }
        }

        public string Name { get; set; }
        public string Table { get; set; }
        public string Table2 { get; set; }
        public string Column { get; set; }
        public string Definition { get; set; }
        public string Type { get; set; }

        public override bool Equals(Object obj)
        {
            return this.Table == ((Constraint)obj).Table && this.Table2 == ((Constraint)obj).Table2 && this.Name == ((Constraint)obj).Name && this.Type == ((Constraint)obj).Type && this.Column == ((Constraint)obj).Column;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Name, Table, Table2, Column, Definition, Type);
        }
    }
}
