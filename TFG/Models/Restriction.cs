using System;
using System.Collections.Generic;

namespace TFG.Models
{
    public class Restriction
    {
        public Restriction(string table, string column1, string column2)
        {
            this.table = table;
            this.column1 = column1;
            this.column2 = column2;
        }

        public string table { get; set; }
        public string column1 { get; set; }
        public string column2 { get; set; }

        public override bool Equals(Object obj)
        {
            return this.table == ((Restriction)obj).table && this.column2 == ((Restriction)obj).column2 && this.column1 == ((Restriction)obj).column1;
        }

    }
}
