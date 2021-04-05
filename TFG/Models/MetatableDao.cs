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
        public Interchange info { get; set; }
        public SqlConnection con { get; set; }

        public MetatableDao(Metatable tabledata, SqlConnection con)
        {
            this.tabledata = tabledata;
            this.con = con;
        }

        // This method creates the needed scripts
        public void loadScripts()
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
        }

        // This method applies the selected masks to the records and saves the results in the corresponding Model variable
        public void getMaskedRecords()
        {
            Dictionary<string, string[]> res = info.Records;

            foreach (KeyValuePair<string, string> entry in info.MasksSelected)
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
                    res.Add(name, container);
                }
            }
            info.Records = res;
        }

        // This method applies runs queries to calculate the best suitable pks for the given tables
        public void getPks()
        {
            getPrimaryKeys(true);
            getSuggestedKeys();
        }

        // This method applies runs queries to calculate the best suitable pks for the given tables
        private void findPks()
        {
            Dictionary<string, string[]> pks = new Dictionary<string, string[]>();
            Dictionary<string, string[]> suggested_pks = new Dictionary<string, string[]>();

            foreach (string entry in tabledata.Tables)
            {
                suggested_pks.Add(entry, findSuggestedPks(entry));
                pks.Add(entry, findPrimaryKey(entry));
            }

            tabledata.TablePks = pks;
            tabledata.TableSuggestedPks = suggested_pks;
        }

        // This method is used to retrieve the primary keys from tabledata to info
        public void getPrimaryKeys(bool getSpacial)
        {
            Dictionary<string, string[]> pks = new Dictionary<string, string[]>();

            if (info.TablesSelected != null)
            {
                foreach (string entry in info.TablesSelected)
                {
                    pks.Add(entry, getPrimaryKey(entry, getSpacial));
                }
            }
            else
            {
                foreach (string entry in info.ColumnsSelected.Keys.ToArray())
                {
                    pks.Add(entry, getPrimaryKey(entry, getSpacial));
                }
            }
            info.TablePks = pks;
        }

        // This method retrieves the primary key(s) of a table (all or only the non spacial ones)
        private string[] getPrimaryKey(string table, bool getSpacial)
        {
            string[] data = tabledata.TablePks[table];
            if (getSpacial)
            {
                return data;
            }
            return filterSpacial(data);
        }

        // This method gathers the primary key(s) of a table from the database
        private string[] findPrimaryKey(string table)
        {
            string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\getPrimaryKey.sql");
            string sql = System.IO.File.ReadAllText(path);
            DataSet ds = Broker.Instance().Run(new SqlCommand(sql + table + "'", con), "schema");
            DataTable dt = ds.Tables["schema"];
            string[] res = new string[dt.Rows.Count];

            for (int j = 0; j < dt.Rows.Count; j++)
            {
                res[j] = (string)dt.Rows[j][0];
            }
            Array.Sort(res);

            return res;
        }

        // This method is used to retrieve the suggested keys from tabledata to info
        public void getSuggestedKeys()
        {
            Dictionary<string, string[]> suggested_pks = new Dictionary<string, string[]>();

            foreach (string entry in info.TablesSelected)
            {
                suggested_pks.Add(entry, tabledata.TableSuggestedPks[entry]);
            }
            info.TableSuggestedPks = suggested_pks;
        }

        // This method applies the selected masks to the records and saves the results in the corresponding Model variable
        private string[] findSuggestedPks(string table)
        {
            string res = "";
            bool found = false;
            int count = 0;
            int distinct = 0, total = 0;

            string[] columns = getTableColumns(table, true);
            string[] IDcolumns = new string[columns.Length];
            for (int i = 0; i < columns.Length; i++)
            {
                if (columns[i].Contains("ID"))
                {
                    IDcolumns[count] = columns[i];
                    count++;
                }
            }
            string[] aux = new string[count];
            Array.Copy(IDcolumns, 0, aux, 0, count);
            count = 0;
            string[] combinations = getCombinations(aux);
            while (!found && count < combinations.Length)
            {
                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT COUNT(*) FROM (SELECT DISTINCT " + combinations[count] + " FROM " + getTableSchemaName(table) + ") AS internalQuery", con), "distinct");
                distinct = (int)ds.Tables["distinct"].Rows[0][0];
                ds = Broker.Instance().Run(new SqlCommand("SELECT COUNT(*) FROM (SELECT " + combinations[count] + " FROM " + getTableSchemaName(table) + ") AS internalQuery", con), "total");
                total = (int)ds.Tables["total"].Rows[0][0];
                if (distinct == total)
                {
                    found = true;
                    res = combinations[count];
                }
                else
                {
                    count++;
                }
            }
            if (!found)
            {
                count = 0;
                combinations = getCombinations(columns);
            }
            while (!found && count < combinations.Length)
            {
                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT COUNT(*) FROM (SELECT DISTINCT " + combinations[count] + " FROM " + getTableSchemaName(table) + ") AS internalQuery", con), "distinct");
                distinct = (int)ds.Tables["distinct"].Rows[0][0];
                ds = Broker.Instance().Run(new SqlCommand("SELECT COUNT(*) FROM (SELECT " + combinations[count] + " FROM " + getTableSchemaName(table) + ") AS internalQuery", con), "total");
                total = (int)ds.Tables["total"].Rows[0][0];
                if (distinct == total)
                {
                    found = true;
                    res = combinations[count];
                }
                else
                {
                    count++;
                }
            }
            string[] suggested = res.Split(',');
            Array.Sort(suggested);
            return suggested;
        }

        // This method calculates and returns all the possible pk combinations ordered
        private string[] getCombinations(string[] columns)
        {
            int count = (int)Math.Pow(2, columns.Length);
            string[] res = new string[count - 1];
            for (int i = 1; i < count; i++)
            {
                string str = Convert.ToString(i, 2).PadLeft(columns.Length, '0');
                string combination = "";
                for (int j = 0; j < str.Length; j++)
                {
                    if (str[j] == '1')
                    {
                        combination += columns[j];
                        combination += ',';
                    }
                }
                combination = combination.Remove(combination.Length - 1);
                res[i - 1] = combination;
            }

            Array.Sort(res, delegate (string comb1, string comb2)
            {
                int count1 = comb1.Split(',').Length - 1;
                int count2 = comb2.Split(',').Length - 1;
                if (count1 < count2)
                {
                    return -1;
                }
                if (count2 < count1)
                {
                    return 1;
                }
                if (comb1.Contains("ID") && !comb2.Contains("ID"))
                {
                    return -1;
                }
                if (!comb1.Contains("ID") && comb2.Contains("ID"))
                {
                    return 1;
                }
                return string.Compare(comb1, comb2);
            });

            return res;
        }

        // This method is used to retrieve only the necessary pks and their records for some functionalities
        public void getPrimaryKeysRecords()
        {
            foreach (KeyValuePair<string, string[]> entry in info.ColumnsSelected)
            {
                string[] pks = getPrimaryKey(entry.Key, false);
                foreach (string pk in pks)
                {
                    string name = entry.Key + '.' + pk;
                    if (!info.Records.ContainsKey(name))
                    {
                        getRecord(name);
                    }
                }
                if (!info.TablePks.ContainsKey(entry.Key))
                {
                    info.TablePks.Add(entry.Key, pks);
                }
            }

        }

        // This method retrieves the already loaded data from the selected columns
        public void getRecord(string record)
        {
            info.Records.Add(record, tabledata.Records[record]);
        }

        // This method gets the data of the columns from the database
        private void findRecords()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            foreach (KeyValuePair<string, string[]> entry in tabledata.TablesColumns)
            {
                string columnsNames = ArrayToString(entry.Value);

                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT " + columnsNames + " FROM " + getTableSchemaName(entry.Key), con), "records");
                DataTable dt = ds.Tables["records"];
                string[][] container = new string[entry.Value.Length][];

                for (int x = 0; x < dt.Rows.Count; x++)
                {
                    for (int i = 0; i < entry.Value.Length; i++)
                    {
                        if (x == 0)
                        {
                            container[i] = new string[dt.Rows.Count];
                        }
                        container[i][x] = dt.Rows[x][i].ToString();
                    }
                }
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name = entry.Key + '.' + entry.Value[i];
                    res.Add(name, container[i]);
                }
            }
            tabledata.Records = res;
        }

        // This method gathers the names of the tables and their columns from the database and saves the result to the corresponding Model variable
        private Dictionary<string, string[]> getTableAndColumnData()
        {
            string[] tables = getTableData();
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            for (int i = 0; i < tables.Length; i++)
            {
                res.Add(tables[i], getTableColumns(tables[i], false));
            }

            Dictionary<string, string[]> result = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string[]> entry in res.OrderBy(key => key.Key))
            {
                result.Add(entry.Key, entry.Value);
            }

            return result;
        }

        // This method gathers the names of the tables from the database and saves the result to the corresponding Model variable
        private string[] getTableData()
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

            return res;
        }

        // This method gathers the names of the columns of a given table from the database and returns the result
        private string[] getTableColumns(string table, bool getSpacial)
        {
            DataSet dsC;
            DataTable dtC;
            DataRow rows;

            dsC = Broker.Instance().Run(new SqlCommand("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "'", con), "columns");
            dtC = dsC.Tables["columns"];

            string[] aux = new string[dtC.Rows.Count];

            for (int j = 0; j < dtC.Rows.Count; j++)
            {
                aux[j] = (string)dtC.Rows[j][0];
            }

            if (!getSpacial)
            {
                return filterSpacial(aux);
            }

            return aux;
        }

        // This method updates the database with the corresponding changes
        public void update(string data)
        {
            switch (info.Functionality)
            {
                case "data_masking":
                    selectMaskedRecords(data);
                    updateDataMasking();
                    break;
                case "primary_keys":
                    selectPksTables(data);
                    updatePrimaryKeys();
                    break;
            }
        }

        // This method updates the database with the corresponding changes for functionality data_masking
        private void updateDataMasking()
        {
            foreach (KeyValuePair<string, string[]> entry in info.ColumnsSelected)
            {
                string[] pks = getPrimaryKey(entry.Key, false);
                string[][] pk_data = new string[pks.Length][];

                for (int i = 0; i < pks.Length; i++)
                {
                    string aux = entry.Key + '.' + pks[i];
                    pk_data[i] = tabledata.Records[aux];
                }

                foreach (string column in entry.Value)
                {
                    string name = entry.Key + '.' + column;
                    string aux = name + "Masked";
                    string[] data = tabledata.Records[(aux)];
                    string[] data_aux = tabledata.Records[(name)];

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

        // This method updates the database with the corresponding changes for functionality primary_keys
        private void updatePrimaryKeys()
        {
            foreach (string entry in info.TablesSelected)
            {
                string constraint_name = "";
                if (tabledata.TablePks[entry].Length != 0)
                {
                    constraint_name = getPKConstraint(entry);
                    Broker.Instance().Run(new SqlCommand("ALTER TABLE " + getTableSchemaName(entry) + " DROP CONSTRAINT " + constraint_name, con), "dropPK");
                }
                constraint_name = "pk_" + entry;
                foreach (string column in tabledata.TableSuggestedPks[entry])
                {
                    makeNotNull(column, entry);
                    constraint_name += "_";
                    constraint_name += column;
                }
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + getTableSchemaName(entry) + " ADD CONSTRAINT " + constraint_name + " PRIMARY KEY (" + ArrayToString(tabledata.TableSuggestedPks[entry]) + ")", con), "addPK");
            }
        }

        // This method is used to retrieve the already computed available masks for a column
        public void getAvailableMasks()
        {
            Dictionary<string, bool[]> res = new Dictionary<string, bool[]>();
            Dictionary<string, string[]> selection = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string[]> entry in info.ColumnsSelected)
            {
                string[] aux = new string[entry.Value.Length];
                int index = 0;

                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name = entry.Key + '.' + entry.Value[i];

                    if (tabledata.MasksAvailable.ContainsKey(name))
                    {
                        res.Add(name, tabledata.MasksAvailable[name]);
                        aux[index] = entry.Value[i];
                        index++;
                    }
                }
                if (aux != null)
                {
                    string[] other = new string[index];
                    Array.Copy(aux, 0, other, 0, index);
                    selection.Add(entry.Key, other);
                }
            }
            info.MasksAvailable = res;
            info.ColumnsSelected = selection;
        }

        // This method is used to filter the masks available to each column according to the data stored in it
        private void findAvailableMasks()
        {
            Dictionary<string, bool[]> auxDict = new Dictionary<string, bool[]>();

            foreach (KeyValuePair<string, string[]> entry in tabledata.TablesColumns)
            {
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    bool[] res = new bool[4];
                    float[] comply = new float[4];
                    string name = entry.Key + '.' + entry.Value[i];
                    string[] data = tabledata.Records[name];

                    if (data != null)
                    {
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

                        for (int j = 0; j < 4; j++)
                        {
                            float aux = (comply[j] * 100) / data.Length;
                            if (aux >= 50)
                            {
                                res[j] = true;
                            }
                            else
                            {
                                res[j] = false;
                            }
                        }
                        if (res.Contains(true))
                        {
                            auxDict.Add(name, res);
                        }
                    }

                }
            }
            tabledata.MasksAvailable = auxDict;
        }
        public void initMetatable()
        {
            this.tabledata.Tables = getTableData();
            this.tabledata.TablesColumns = getTableAndColumnData();
            findRecords();
            findAvailableMasks();
            findPks();
            System.Diagnostics.Debug.Write("hello");
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

        // This method sets a column to not null
        private void makeNotNull(string column, string table)
        {
            Broker.Instance().Run(new SqlCommand("ALTER TABLE " + getTableSchemaName(table) + " ALTER COLUMN " + column + " " + getDataType(column) + " NOT NULL", con), "type");
        }

        // This method gathers the PK constraint name of a table from the database
        private string getPKConstraint(string table)
        {
            string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\getPKConstraint.sql");
            string sql = System.IO.File.ReadAllText(path);
            DataSet ds = Broker.Instance().Run(new SqlCommand(sql + table + "'", con), "schema");
            DataTable dt = ds.Tables["schema"];
            DataRow row = dt.Rows[0];
            return (string)row[0];
        }

        // This method is used to filter the spacial datatypes 
        private string[] filterSpacial(string[] array)
        {
            int index = array.Length;
            string[] res = new string[index];

            for (int j = 0; j < array.Length; j++)
            {
                string type = getDataType(array[j]);
                if (!isSpacial(type))
                {
                    res[j] = array[j];
                }
                else
                {
                    index--;
                }
            }

            string[] other = new string[index];
            index = 0;

            for (int j = 0; j < array.Length; j++)
            {
                if (res[j] != null)
                {
                    other[index] = res[j];
                    index++;
                }
            }
            res = other;

            Array.Sort(res);
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

        // This method saves the selection of the records selected for the data_masking functionality
        private void selectMaskedRecords(string data)
        {
            Dictionary<string, string[]> res = parseColumnSelection(data);

            foreach (KeyValuePair<string, string[]> record in tabledata.Records)
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
                                aux_masked[counter] = tabledata.Records[record.Key + "Masked"][i];
                                counter++;
                            }
                        }

                        tabledata.Records[record.Key] = aux;
                        tabledata.Records[record.Key + "Masked"] = aux_masked;
                    }
                }
            }
        }

        // This method saves the selection of the tables selected for the primary_keys functionality
        private void selectPksTables(string data)
        {
            string[] aux = data.Split(',');
            string[] tables = new string[aux.Length - 1];
            Array.Copy(aux, 1, tables, 0, aux.Length - 1);
            Dictionary<string, string[]> pks = new Dictionary<string, string[]>();
            Dictionary<string, string[]> suggestedPks = new Dictionary<string, string[]>();

            foreach (string table in tables)
            {
                if (ArrayToString(tabledata.TablePks[table]) != ArrayToString(tabledata.TableSuggestedPks[table]))
                {
                    pks.Add(table, tabledata.TablePks[table]);
                    suggestedPks.Add(table, tabledata.TableSuggestedPks[table]);
                }
            }
            info.TablesSelected = pks.Keys.ToArray();
            info.TablePks = pks;
            info.TableSuggestedPks = suggestedPks;
        }

        // This method parses a string into a dictionary
        public Dictionary<string, string[]> parseColumnSelection(string selection)
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

        // This method parses a string into an array
        public string[] parseTableSelection(string selection)
        {
            string[] tables = selection.Split('/');
            string[] res = new string[tables.Length - 1];
            Array.Copy(tables, 1, res, 0, res.Length);
            return res;
        }

        //
        private string ArrayToString(object[] array)
        {
            string res = "";
            for (int i = 0; i < array.Length; i++)
            {
                if (i == 0)
                {
                    res = '[' + array[i].ToString() + ']';
                }
                else
                {
                    res += '[';
                    res += array[i].ToString();
                    res += ']';
                }
                if (i != array.Length - 1)
                {
                    res += ',';
                }
            }
            return res;
        }
    }
}
