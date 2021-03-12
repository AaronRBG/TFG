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
    public class MetatableDao
    {

        public Metatable tabledata { get; set; }
        public SqlConnection con { get; set; }

        public MetatableDao(Metatable tabledata, SqlConnection con)
        {
            this.tabledata = tabledata;
            this.con = con;
        }

        public void saveSelections(string selection)
        {
            tabledata.ColumnsSelected = parseSelection(selection);
        }

        private Dictionary<string, string[]> parseSelection(string selection)
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

        public void saveTypes(string data, Boolean deleteSelection)
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
            tabledata.types = types;
            if (deleteSelection)
            {
                tabledata.ColumnsSelected = selection;
            }
            getMaskedRecords();
        }

        public string getTableSchemaName(string table)
        {
            DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT '[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '" + table + "'", con), "schema");
            DataTable dt = ds.Tables["schema"];
            DataRow row = dt.Rows[0];
            return (string)row[0];
        }

        public string getDataType(string column)
        {
            DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '" + column + "'", con), "type");
            DataTable dt = ds.Tables["type"];
            DataRow row = dt.Rows[0];
            return (string)row[0];
        }

        public string[] getPrimaryKey(string table)
        {
            string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\getPrimaryKey.sql");
            string sql = System.IO.File.ReadAllText(path);
            DataSet ds = Broker.Instance().Run(new SqlCommand(sql + table + "'", con), "schema");
            DataTable dt = ds.Tables["schema"];
            string[] res = new string[dt.Rows.Count];

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                res[i] = (string)dt.Rows[i][0];
            }

            return res;
        }

        public void getMaskedRecords()
        {
            getPrimaryKeysRecords();
            Dictionary<string, string[]> res = tabledata.records;

            foreach (KeyValuePair<string, string> entry in tabledata.types)
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

                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT [" + mask + pair[1] + "]) FROM " + getTableSchemaName(pair[0]), con), "records");
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
            tabledata.records = res;
        }

        public void getPrimaryKeysRecords()
        {
            Dictionary<string, string[]> res = tabledata.records;
            Dictionary<string, string[]> tablePks = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string> entry in tabledata.types)
            {
                string[] table = entry.Key.Split('.');

                string[] pks = getPrimaryKey(table[0]);
                tablePks.Add(table[0], pks);

                for (int j = 0; j < pks.Length; j++)
                {
                    DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT [" + pks[j] + "] FROM " + getTableSchemaName(table[0]), con), "records");
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
            tabledata.records = res;
            tabledata.tablePks = tablePks;
        }

        public Dictionary<string, string[]> getRecords(string record)
        {
            Dictionary<string, string[]> res = tabledata.records;

            if (!res.ContainsKey(record))
            {

                string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\createDNIMask.sql");
                string sql = System.IO.File.ReadAllText(path);
                Broker.Instance().Run(new SqlCommand(sql, con), "createFunctions");
                path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\createPhoneMask.sql");
                sql = System.IO.File.ReadAllText(path);
                Broker.Instance().Run(new SqlCommand(sql, con), "createFunctions");
                path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\createEmailMask.sql");
                sql = System.IO.File.ReadAllText(path);
                Broker.Instance().Run(new SqlCommand(sql, con), "createFunctions");
                path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\createCreditCardMask.sql");
                sql = System.IO.File.ReadAllText(path);
                Broker.Instance().Run(new SqlCommand(sql, con), "createFunctions");

                string[] column = record.Split('.');

                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT [" + column[1] + "] FROM " + getTableSchemaName(column[0]), con), "records");
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

        public Dictionary<string, string[]> getTableAndColumnData()
        {
            // then it runs a query that returns all the tables names from that database
            // then processes the result to run a nested for loop which itself runs another query that returns all the column names of that table
            // when the for loop is finished we have a double string array which stores for each table its name and the names of all its columns, then saves this in the viewbag

            DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", con), "tables");
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
                dsC = Broker.Instance().Run(new SqlCommand("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + rows[0] + "'", con), "columns");
                dtC = dsC.Tables["columns"];

                aux = new string[dtC.Rows.Count];
                tablename = (string)rows[0];

                int index = dtC.Rows.Count;

                for (int j = 0; j < dtC.Rows.Count; j++)
                {
                    rows = dtC.Rows[j];
                    string type = getDataType((string)rows[0]);
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

                Array.Sort<string>(aux);

                dsC.Reset();
                res.Add(tablename, aux);
            }

            Dictionary<string, string[]> result = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string[]> entry in res.OrderBy(key => key.Key))
            {
                result.Add(entry.Key, entry.Value);
            }

            return result;
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

        public void getAvailableMasks(string column)
        {
            bool[] res = new bool[4];
            float[] comply = new float[4];
            string[] data = tabledata.records[column];

            foreach (string value in data)
            {

                if (isDNI(value))
                {
                    comply[0]++;
                }

                if (isEmail(value))
                {
                    comply[1]++;
                }

                if (isPhone(value))
                {
                    comply[2]++;
                }

                if (isCCN(value))
                {
                    comply[3]++;
                }

            }

            for (int i = 0; i < 4; i++)
            {
                float aux = (comply[i] * 100) / data.Length;
                if (aux >= 50)
                {
                    res[i] = true;
                }
                else
                {
                    res[i] = false;
                }
            }

            tabledata.masksAvailable.Add(column, res);
        }

        private static bool isDNI(string value)
        {
            char[] aux = value.ToCharArray();
            int INTcount = 0;
            int LETTERcount = 0;
            foreach (char i in aux)
            {
                if (Char.IsDigit(i))
                {
                    INTcount++;
                }
                if (Char.IsLetter(i))
                {
                    LETTERcount++;
                }
            }
            if ((INTcount == 8 || INTcount == 9) && LETTERcount == 1)
            {
                return true;
            }
            return false;
        }
        private bool isEmail(string value)
        {
            if (value.Contains('@') && value.Contains('.') && value.IndexOf('@') < value.IndexOf('.'))
            {
                return true;
            }
            return false;
        }
        private bool isPhone(string value)
        {
            char[] aux = value.ToCharArray();
            int count = 0;
            foreach (char i in aux)
            {
                if (Char.IsDigit(i))
                {
                    count++;
                }
            }
            if (count >= 7 && count <= 15)
            {
                return true;
            }
            return false;
        }

        private bool isCCN(string value)
        {
            char[] aux = value.ToCharArray();
            int count = 0;
            foreach (char i in aux)
            {
                if (Char.IsDigit(i))
                {
                    count++;
                }
            }
            if (count >= 13 && count <= 19)
            {
                return true;
            }
            return false;
        }

        public void update()
        {
            foreach (KeyValuePair<string, string[]> entry in tabledata.ColumnsSelected)
            {
                string[] pks = getPrimaryKey(entry.Key);
                string[][] pk_data = new string[pks.Length][];

                for (int i = 0; i < pks.Length; i++)
                {
                    string aux = entry.Key + '.' + pks[i];
                    pk_data[i] = tabledata.records[aux];
                }

                foreach (string column in entry.Value)
                {
                    string name = entry.Key + '.' + column;
                    string aux = name + "Masked";
                    string[] data = tabledata.records[(aux)];
                    string[] data_aux = tabledata.records[(name)];

                    for (int i = 0; i < data.Length; i++)
                    {
                        string str = " WHERE";

                        for (int j = 0; j < pks.Length; j++)
                        {
                            string type = getDataType(pks[j]);
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
                        if (pks.Length == 0)
                        {
                            string type = getDataType(column);
                            str += " " + column + " = convert(" + type + ", '" + data_aux[i];
                            if (type == "datetime")
                            {
                                str += "', 103)";
                            }
                            else
                            {
                                str += "')";
                            }
                        }

                        Broker.Instance().Run(new SqlCommand("UPDATE " + getTableSchemaName(entry.Key) + " SET " + column + " = '" + data[i] + "'" + str, con), "update");
                    }
                }
            }
        }

        public void selectRows(string data)
        {
            Dictionary<string, string[]> res = parseSelection(data);

            foreach (KeyValuePair<string, string[]> record in tabledata.records)
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
                                aux_masked[counter] = tabledata.records[record.Key + "Masked"][i];
                                counter++;
                            }
                        }

                        tabledata.records[record.Key] = aux;
                        tabledata.records[record.Key + "Masked"] = aux_masked;
                    }
                }
            }
        }
    }
}
