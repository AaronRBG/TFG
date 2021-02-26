using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
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
            selections[id].ColumnsSelected = parseSelection(selection);
        }

        public Dictionary<string, string[]> parseSelection(string selection)
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            string[] tables = selection.Split('/');
            for (int i = 1; i < tables.Length; i++)
            {
                string[] columns = tables[i].Split(',');
                res.Add(columns[0], columns.Skip(1).ToArray());
            }

            return res;
        }

        public void saveTypes(string id, string data, Boolean deleteSelection)
        {
            Dictionary<string, string> types = new Dictionary<string, string>();
            Dictionary<string, string[]> selection = new Dictionary<string, string[]>();

            string[] columns = data.Split('/');
            for (int i = 1; i < columns.Length; i++)
            {
                string[] names = columns[i].Split(',');
                types.Add(names[0], names[1]);

                string[] pair = names[0].Split('.');
                if (deleteSelection)
                {
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
            }
            selections[id].types = types;
            if (deleteSelection)
            {
                selections[id].ColumnsSelected = selection;
            }
            getMaskedRecords(id);
        }

        public string getTableSchemaName(string id, string table)
        {
            DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT '[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '" + table + "'", connections[id]), "schema");
            DataTable dt = ds.Tables["schema"];
            DataRow row = dt.Rows[0];
            return (string)row[0];
        }

        public string getDataType(string id, string column)
        {
            DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '" + column + "'", connections[id]), "type");
            DataTable dt = ds.Tables["type"];
            DataRow row = dt.Rows[0];
            return (string)row[0];
        }

        public string[] getPrimaryKey(string id, string table)
        {
            string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\getPrimaryKey.sql");
            string sql = System.IO.File.ReadAllText(path);
            DataSet ds = Broker.Instance().Run(new SqlCommand(sql + table + "'", connections[id]), "schema");
            DataTable dt = ds.Tables["schema"];
            string[] res = new string[dt.Rows.Count];

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                res[i] = (string)dt.Rows[i][0];
            }

            return res;
        }

        public void getMaskedRecords(string id)
        {
            getPrimaryKeysRecords(id);
            Dictionary<string, string[]> res = selections[id].records;

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
                string name = entry.Key + "Masked";
                if (!res.ContainsKey(name))
                {
                    res.Add((name), container);
                }
            }
            selections[id].records = res;
        }

        public void getPrimaryKeysRecords(string id)
        {
            Dictionary<string, string[]> res = selections[id].records;

            foreach (KeyValuePair<string, string> entry in selections[id].types)
            {
                string[] table = entry.Key.Split('.');

                string[] pks = getPrimaryKey(id, table[0]);

                for (int j = 0; j < pks.Length; j++)
                {
                    DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT [" + pks[j] + "] FROM " + getTableSchemaName(id, table[0]), connections[id]), "records");
                    DataTable dt = ds.Tables["records"];
                    String[] container = new string[dt.Rows.Count];
                    for (int x = 0; x < dt.Rows.Count; x++)
                    {
                        container[x] = dt.Rows[x][0].ToString();
                    }
                    string key = table[0] + '.' + pks[j];
                    if (!res.ContainsKey(key))
                    {
                        res.Add(key, container);
                    }
                }
            }
            selections[id].records = res;
        }

        public Dictionary<string, string[]> getRecords(string id, string record)
        {
            Dictionary<string, string[]> res = Manager.Instance().selections[id].records;

            if (!res.ContainsKey(record))
            {

                string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\createDNIMask.sql");
                string sql = System.IO.File.ReadAllText(path);
                Broker.Instance().Run(new SqlCommand(sql, connections[id]), "createFunctions");
                path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\createPhoneMask.sql");
                sql = System.IO.File.ReadAllText(path);
                Broker.Instance().Run(new SqlCommand(sql, connections[id]), "createFunctions");
                path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\createEmailMask.sql");
                sql = System.IO.File.ReadAllText(path);
                Broker.Instance().Run(new SqlCommand(sql, connections[id]), "createFunctions");
                path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\createCreditCardMask.sql");
                sql = System.IO.File.ReadAllText(path);
                Broker.Instance().Run(new SqlCommand(sql, connections[id]), "createFunctions");

                string[] column = record.Split('.');

                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT [" + column[1] + "] FROM " + getTableSchemaName(id, column[0]), connections[id]), "records");
                DataTable dt = ds.Tables["records"];
                String[] container = new string[dt.Rows.Count];
                for (int x = 0; x < dt.Rows.Count; x++)
                {
                    container[x] = dt.Rows[x][0].ToString();
                }
                res.Add(record, container);
            }
            return res;
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

                int index = dtC.Rows.Count;

                for (int j = 0; j < dtC.Rows.Count; j++)
                {
                    rows = dtC.Rows[j];
                    string type = getDataType(id, (string)rows[0]);
                    if (!isSpacial(type))
                    {
                        aux[j] = (string)rows[0];
                    }
                    else
                    {
                        index--;
                    }
                }

                string[] other = new string[index];
                index = 0;

                for (int j = 0; j < dtC.Rows.Count; j++)
                {
                    if (aux[j] != null)
                    {
                        other[index] = aux[j];
                        index++;
                    }
                }
                aux = other;

                dsC.Reset();
                res.Add(tablename, aux);
            }

            return res;
        }

        private bool isSpacial(string type)
        {
            if (type == "geometry")
            {
                return true;
            }
            if (type == "geography")
            {
                return true;
            }
            if (type == "hierarchyid")
            {
                return true;
            }
            return false;
        }

        public void update(string id)
        {
            foreach (KeyValuePair<string, string[]> entry in Manager.Instance().selections[id].ColumnsSelected)
            {
                string[] pks = getPrimaryKey(id, entry.Key);
                string[][] pk_data = new string[pks.Length][];

                for (int i = 0; i < pks.Length; i++)
                {
                    string aux = entry.Key + '.' + pks[i];
                    pk_data[i] = selections[id].records[aux];
                }

                foreach (string column in entry.Value)
                {
                    string aux = entry.Key + '.' + column + "Masked";
                    string[] data = selections[id].records[(aux)];

                    for (int i = 0; i < data.Length; i++)
                    {
                        string str = "";

                        for (int j = 0; j < pks.Length; j++)
                        {
                            string type = getDataType(id, pks[j]);
                            str += " " + pks[j] + " = convert(" + type + ", '" + pk_data[j][i];
                            if (type == "datetime")
                            {
                                str += "', 103)";
                            }
                            else
                            {
                                str += "')";
                            }
                            if (j != pks.Length - 1)
                            {
                                str += " and";
                            }
                        }

                        Broker.Instance().Run(new SqlCommand("UPDATE " + getTableSchemaName(id, entry.Key) + " SET " + column + " = " + data[i] + " WHERE" + str, connections[id]), "update");
                    }
                }
            }
        }

        internal void selectRows(string data, string id)
        {
            Dictionary<string, string[]> res = parseSelection(data);

            foreach (KeyValuePair<string, string[]> record in selections[id].records)
            {
                foreach (KeyValuePair<string, string[]> entry in res)
                {
                    if (record.Key == entry.Key)
                    {
                        string[] aux = new string[entry.Value.Length];
                        string[] aux_masked = new string[entry.Value.Length];
                        int counter = 0;
                        for (int i = 0; i < record.Value.Length; i++)
                        {
                            if (entry.Value.Contains(i.ToString()))
                            {
                                aux[counter] = record.Value[i];
                                aux_masked[counter] = selections[id].records[record.Key + "Masked"][i];
                                counter++;
                            }
                        }

                        selections[id].records[record.Key] = aux;
                        selections[id].records[record.Key+"Masked"] = aux_masked;
                    }
                }
            }
        }
    }
}
