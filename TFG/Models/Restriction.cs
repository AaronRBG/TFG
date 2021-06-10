using System;
using System.Collections.Generic;

namespace TFG.Models
{
    public class Restriction
    {
        public Restriction(string table, string column1, string column2)
        {
            this.Table = table;
            this.Column1 = column1;
            this.Column2 = column2;
        }

        public string Table { get; set; }
        public string Column1 { get; set; }
        public string Column2 { get; set; }

        public override bool Equals(Object obj)
        {
            return this.Table == ((Restriction)obj).Table && this.Column2 == ((Restriction)obj).Column2 && this.Column1 == ((Restriction)obj).Column1;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Table, Column1, Column2);
        }
    }
}
