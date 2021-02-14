using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using TFG.Models;

namespace TFG
{
    public sealed class Manager
    {
        public Dictionary<string, SqlConnection> connections { get; }
        public Dictionary<string, ScriptsResults> selections { get; }

        private static Manager instance;

        public Manager()
        {
            connections = new Dictionary<string, SqlConnection>();
            selections = new Dictionary<string, ScriptsResults>();

        }

        public static Manager Instance()

        {
            if (instance == null)
            {
                instance = new Manager();
            }
            return instance;
        }

        public void saveSelections(string selection, string id)
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            string[] tables = selection.Split('/');
            for (int i = 1; i < tables.Length; i++)
            {
                string[] columns = tables[i].Split(',');
                res.Add(columns[0], columns.Skip(1).ToArray());
            }
            selections[id].ColumnsSelected = res;
        }

        public void saveTypes(string id, string data)
        {
            Dictionary<string, string> types = new Dictionary<string, string>();
            Dictionary<string, string[]> selection = new Dictionary<string, string[]>();

            string[] columns = data.Split('/');
            for (int i = 1; i < columns.Length; i++)
            {
                string[] names = columns[i].Split(',');
                types.Add(names[0], names[1]);

                string[] pair = names[0].Split('.');
                if (selection.ContainsKey(pair[0]))
                {
                    string[] aux = new string[selection[pair[0]].Length + 1];
                    for (int j = 0; j < selection[pair[0]].Length; j++)
                    {
                        aux[j] = selection[pair[0]][j];
                    }
                    aux[aux.Length - 1] = pair[1];
                    selection[pair[0]] = aux;
                }
                else
                {
                    string[] aux = { pair[1] };
                    selection.Add(pair[0], aux);
                }
            }
            selections[id].types = types;
            selections[id].ColumnsSelected = selection;
            getMaskedRecords(id);
        }

        public string getTableSchemaName(string id, string table)
        {
            DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT '[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '" + table + "'", connections[id]), "schema");
            DataTable dt = ds.Tables["schema"];
            DataRow row = dt.Rows[0];
            return (string)row[0];
        }

        public void getMaskedRecords(string id)
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string> entry in Manager.Instance().selections[id].types)
            {
                string[] pair = entry.Key.Split('.');
                string mask;
                switch (entry.Value)
                {
                    case "DNI":
                        mask = "dbo].[DNIMask]([";
                        break;
                    case "Phone Number":
                        mask = "dbo].[phoneMask]([";
                        break;
                    case "Credit card":
                        mask = "dbo].[creditCardMask]([";
                        break;
                    default:
                        mask = "dbo].[emailMask]([";
                        break;
                }

                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT [" + mask + pair[1] + "]) FROM " + getTableSchemaName(id, pair[0]), connections[id]), "records");
                DataTable dt = ds.Tables["records"];
                String[] container = new string[dt.Rows.Count];
                for (int x = 0; x < dt.Rows.Count; x++)
                {
                    container[x] = dt.Rows[x][0].ToString();
                }
                res.Add(entry.Key, container);
            }
            selections[id].records = res;
        }

        public void getRecords(string id)
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string[]> entry in Manager.Instance().selections[id].ColumnsSelected)
            {
                for (int j = 0; j < entry.Value.Length; j++)
                {
                    DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT [" + entry.Value[j] + "] FROM " + getTableSchemaName(id, entry.Key), connections[id]), "records");
                    DataTable dt = ds.Tables["records"];
                    String[] container = new string[dt.Rows.Count];
                    for (int x = 0; x < dt.Rows.Count; x++)
                    {
                        container[x] = dt.Rows[x][0].ToString();
                    }
                    string key = entry.Key + '.' + entry.Value[j];
                    res.Add(key, container);
                }
            }
            selections[id].records = res;
        }

        public Dictionary<string, string[]> getTableAndColumnData(string id)
        {
            // then it runs a query that returns all the tables names from that database
            // then processes the result to run a nested for loop which itself runs another query that returns all the column names of that table
            // when the for loop is finished we have a double string array which stores for each table its name and the names of all its columns, then saves this in the viewbag

            DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", connections[id]), "tables");
            DataSet dsC;
            DataTable dt = ds.Tables["tables"];
            DataTable dtC;
            DataRow rows;
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            string[] aux;
            string tablename;

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                rows = dt.Rows[i];
                dsC = Broker.Instance().Run(new SqlCommand("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + rows[0] + "'", connections[id]), "columns");
                dtC = dsC.Tables["columns"];

                aux = new string[dtC.Rows.Count];
                tablename = (string)rows[0];

                for (int j = 0; j < dtC.Rows.Count; j++)
                {
                    rows = dtC.Rows[j];
                    aux[j] = (string)rows[0];
                }
                dsC.Reset();
                res.Add(tablename, aux);
            }

            return res;
        }

    }
}
