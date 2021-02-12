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
            string[][] selected;

            string[] tables = selection.Split('/');
            selected = new string[tables.Length - 1][];
            for (int i = 1; i < tables.Length; i++)
            {
                string[] columns = tables[i].Split(',');
                selected[i - 1] = columns;
            }
            selections[id].results = selected;
        }

        public string[][] getTableAndColumnData(string id)
        {
            // then it runs a query that returns all the tables names from that database
            // then processes the result to run a nested for loop which itself runs another query that returns all the column names of that table
            // when the for loop is finished we have a double string array which stores for each table its name and the names of all its columns, then saves this in the viewbag

            DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", connections[id]), "tables");
            DataSet dsC;
            DataTable dt = ds.Tables["tables"];
            DataTable dtC;
            DataRow rows;
            string[][] res = new string[dt.Rows.Count][];

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                rows = dt.Rows[i];
                dsC = Broker.Instance().Run(new SqlCommand("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + rows[0] + "'", connections[id]), "columns");
                dtC = dsC.Tables["columns"];

                res[i] = new string[dtC.Rows.Count + 1];
                res[i][0] = (string)rows[0];

                for (int j = 1; j < dtC.Rows.Count + 1; j++)
                {
                    rows = dtC.Rows[j - 1];
                    res[i][j] = (string)rows[0];
                }
                dsC.Reset();
            }

            return res;
        }

    }
}
