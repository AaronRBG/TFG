using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace TFG.Models
{
    public class Performance
    {
        public Performance(string database)
        {
            this.Database = database;
            this.Log = new List<string>();
            this.Rows = new Dictionary<string, string[]>();
            this.Reserved = new Dictionary<string, string[]>();
            this.Data = new Dictionary<string, string[]>();
            this.Index_size = new Dictionary<string, string[]>();
            this.Unused = new Dictionary<string, string[]>();
            this.Query_time = new Dictionary<string, string[]>();
        }

        public string Database { get; set; }
        public List<string> Log { get; set; }
        public Dictionary<string, string[]> Rows { get; set; }
        public Dictionary<string, string[]> Reserved { get; set; }
        public Dictionary<string, string[]> Data { get; set; }
        public Dictionary<string, string[]> Index_size { get; set; }
        public Dictionary<string, string[]> Unused { get; set; }
        public Dictionary<string, string[]> Query_time { get; set; }

        public void InsertFirst(string table, string rows, string reserved, string data, string index_size, string unused, string query_time)
        {
            string[] aux = new string[2];
            aux[0] = rows;
            aux[1] = aux[0];
            this.Rows.Add(table, aux);

            aux = new string[2];
            aux[0] = reserved;
            aux[1] = aux[0];
            this.Reserved.Add(table, aux);

            aux = new string[2];
            aux[0] = data;
            aux[1] = aux[0];
            this.Data.Add(table, aux);

            aux = new string[2];
            aux[0] = index_size;
            aux[1] = aux[0];
            this.Index_size.Add(table, aux);

            aux = new string[2];
            aux[0] = unused;
            aux[1] = aux[0];
            this.Unused.Add(table, aux);

            aux = new string[2];
            aux[0] = query_time;
            aux[1] = aux[0];
            this.Query_time.Add(table, aux);
        }

        public void InsertLater(string table, string rows, string reserved, string data, string index_size, string unused, string query_time)
        {
            this.Rows[table][1] = rows;
            this.Reserved[table][1] = reserved;
            this.Data[table][1] = data;
            this.Index_size[table][1] = index_size;
            this.Unused[table][1] = unused;
            this.Query_time[table][1] = query_time;
        }

    }
}
