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
            this.database = database;
            this.log = new List<string>();
            this.rows = new Dictionary<string, string[]>();
            this.reserved = new Dictionary<string, string[]>();
            this.data = new Dictionary<string, string[]>();
            this.index_size = new Dictionary<string, string[]>();
            this.unused = new Dictionary<string, string[]>();
            this.query_time = new Dictionary<string, string[]>();
        }

        public string database;
        public List<string> log;
        public Dictionary<string, string[]> rows = new Dictionary<string, string[]>();
        public Dictionary<string, string[]> reserved = new Dictionary<string, string[]>();
        public Dictionary<string, string[]> data = new Dictionary<string, string[]>();
        public Dictionary<string, string[]> index_size = new Dictionary<string, string[]>();
        public Dictionary<string, string[]> unused = new Dictionary<string, string[]>();
        public Dictionary<string, string[]> query_time = new Dictionary<string, string[]>();

        public void insertFirst(string table, string rows, string reserved, string data, string index_size, string unused, string query_time)
        {
            string[] aux = new string[2];
            aux[0] = rows;
            aux[1] = aux[0];
            this.rows.Add(table, aux);

            aux = new string[2];
            aux[0] = reserved;
            aux[1] = aux[0];
            this.reserved.Add(table, aux);

            aux = new string[2];
            aux[0] = data;
            aux[1] = aux[0];
            this.data.Add(table, aux);

            aux = new string[2];
            aux[0] = index_size;
            aux[1] = aux[0];
            this.index_size.Add(table, aux);

            aux = new string[2];
            aux[0] = unused;
            aux[1] = aux[0];
            this.unused.Add(table, aux);

            aux = new string[2];
            aux[0] = query_time;
            aux[1] = aux[0];
            this.query_time.Add(table, aux);
        }

        public void insertLater(string table, string rows, string reserved, string data, string index_size, string unused, string query_time)
        {
            this.rows[table][1] = rows;
            this.reserved[table][1] = reserved;
            this.data[table][1] = data;
            this.index_size[table][1] = index_size;
            this.unused[table][1] = unused;
            this.query_time[table][1] = query_time;
        }

    }
}
