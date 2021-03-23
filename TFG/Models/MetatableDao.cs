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
        // The variables are the Model in which the methods results are saved and the connection to the database
        public Metatable tabledata { get; set; }
        public SqlConnection con { get; set; }

        public MetatableDao(Metatable tabledata, SqlConnection con)
        {
            this.tabledata = tabledata;
            this.con = con;
        }

        // This method applies the selected masks to the records and saves the results in the corresponding Model variable
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
                string[] container = new string[dt.Rows.Count];
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

        // This method creates the needed scripts and gets the data of the column from the database
        public void getRecord(string record)
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
                tabledata.records = res;
            }
        }
        
        // This method gathers the names of the tables and their columns from the database and saves the result to the corresponding Model variable
        public void getTableAndColumnData()
        {

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

            tabledata.ColumnsSelected = result;
        }

        // This method gathers the names of the tables from the database and saves the result to the corresponding Model variable
        public void getTableData()
        {

            DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", con), "tables");
            DataTable dt = ds.Tables["tables"];
            DataRow rows;
            string[] res = new string[dt.Rows.Count];

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                rows = dt.Rows[i];
                res[i] = (string)rows[0];
            }

            Array.Sort<string>(res);

            tabledata.TablesSelected = res;
        }

        // This method updates the database with the corresponding changes
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

        // This method is used to filter the masks available to each column according to the data stored in it
        public void getAvailableMasks(string column)
        {
            bool[] res = new bool[4];
            float[] comply = new float[4];
            string[] data = tabledata.records[column];

            if (data.Length > 1000)
            {
                string[] result = new string[1000];
                Array.Copy(data, 0, result, 0, 1000);
                data = result;
            }

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

        // This method gets the data of the primary key(s) columns and saves the results in the corresponding Model variable
        private void getPrimaryKeysRecords()
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

        // This method gathers the schema name of a table from the database
        private string getTableSchemaName(string table)
        {
            DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT '[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '" + table + "'", con), "schema");
            DataTable dt = ds.Tables["schema"];
            DataRow row = dt.Rows[0];
            return (string)row[0];
        }

        // This method gathers the datatype of a column from the database
        private string getDataType(string column)
        {
            DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '" + column + "'", con), "type");
            DataTable dt = ds.Tables["type"];
            DataRow row = dt.Rows[0];
            return (string)row[0];
        }

        // This method gathers the primary key(s) of a table from the database
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

        // This method is used to filter the spacial datatypes 
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

        // This method is used to check if a database cell data corresponds to a DNI
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

        // This method is used to check if a database cell data corresponds to an Email
        private bool isEmail(string value)
        {
            if (value.Contains('@') && value.Contains('.') && value.IndexOf('@') < value.IndexOf('.'))
            {
                return true;
            }
            return false;
        }

        // This method is used to check if a database cell data corresponds to a Phone Number
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

        // This method is used to check if a database cell data corresponds to a Credit Card Number
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

    }
}
