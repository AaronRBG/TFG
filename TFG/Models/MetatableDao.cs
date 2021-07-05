using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using TFG.Models;

namespace TFG
{
    public class MetatableDao
    {
        // The variables are the Model in which the methods results are saved and the connection to the database
        public Metatable Tabledata { get; set; }
        public Interchange Info { get; set; }
        public Performance Perf { get; set; }
        public Help Help { get; set; }
        public SqlConnection Con { get; set; }

        public MetatableDao(Metatable tabledata, SqlConnection con)
        {
            this.Tabledata = tabledata;
            this.Con = con;
            this.Help = new Help();
        }

        // This method creates the needed scripts
        public void LoadScripts()
        {
            string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\createDNIMask.sql");
            string sql = System.IO.File.ReadAllText(path);
            Broker.Instance().Run(new SqlCommand(sql, Con), "createFunctions");
            path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\createPhoneMask.sql");
            sql = System.IO.File.ReadAllText(path);
            Broker.Instance().Run(new SqlCommand(sql, Con), "createFunctions");
            path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\createEmailMask.sql");
            sql = System.IO.File.ReadAllText(path);
            Broker.Instance().Run(new SqlCommand(sql, Con), "createFunctions");
            path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\createCreditCardMask.sql");
            sql = System.IO.File.ReadAllText(path);
            Broker.Instance().Run(new SqlCommand(sql, Con), "createFunctions");
        }

        // This method applies the selected masks to the records and saves the results in the corresponding Model variable
        public void GetMaskedRecords()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string> entry in Info.MasksSelected)
            {
                string[] pair = entry.Key.Split('.');
                string mask = entry.Value switch
                {
                    "DNI" => "dbo].[DNIMask]([",
                    "Phone Number" => "dbo].[phoneMask]([",
                    "Credit card" => "dbo].[creditCardMask]([",
                    _ => "dbo].[emailMask]([",
                };
                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT [" + mask + pair[1] + "]) FROM " + GetTableSchemaName(pair[0]), Con), "records");
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
            foreach (KeyValuePair<string, string[]> entry in res)
            {
                Info.Records.Add(entry.Key, entry.Value);
            }
        }

        // This method applies runs queries to calculate the best suitable pks for the given tables
        public void GetPks()
        {
            GetPrimaryKeys(true);
            GetSuggestedKeys();
        }

        // This method applies runs queries to calculate the best suitable pks for the given tables
        private void FindPks()
        {
            Dictionary<string, string[]> pks = new Dictionary<string, string[]>();
            Dictionary<string, string[]> suggested_pks = new Dictionary<string, string[]>();

            foreach (string entry in Tabledata.Tables)
            {
                suggested_pks.Add(entry, FindSuggestedPks(entry));
                pks.Add(entry, FindPrimaryKey(entry));
            }

            Tabledata.TablePks = pks;
            Tabledata.TableSuggestedPks = suggested_pks;
        }

        // This method applies runs queries to calculate the best suitable fks for the given tables
        public void GetFks()
        {
            GetForeignKeys();
            GetSuggestedFks();
        }

        // This method applies runs queries to calculate the best suitable fks for the given tables
        private void FindFks()
        {
            Models.Constraint[] aux = Tabledata.Constraints.Where(c => c.Type == "FOREIGN KEY").ToArray();
            foreach (Models.Constraint c in aux)
            {
                c.Table = c.Table.Replace("[", "").Replace("]", "");
                if (c.Table.Contains('.'))
                {
                    c.Table = c.Table.Split('.')[1];
                }
                c.Table2 = c.Table2.Replace("[", "").Replace("]", "");
                if (c.Table2.Contains('.'))
                {
                    c.Table2 = c.Table2.Split('.')[1];
                }
            }
            Tabledata.TableFks = aux;
            Tabledata.TableSuggestedFks = FindSuggestedFks();
        }

        // This method is used to retrieve the primary keys from tabledata to info
        public void GetPrimaryKeys(bool getSpacial)
        {
            Dictionary<string, string[]> pks = new Dictionary<string, string[]>();

            if (Info.TablesSelected != null)
            {
                foreach (string entry in Info.TablesSelected)
                {
                    pks.Add(entry, GetPrimaryKey(entry, getSpacial));
                }
            }
            else
            {
                foreach (string entry in Info.ColumnsSelected.Keys.ToArray())
                {
                    pks.Add(entry, GetPrimaryKey(entry, getSpacial));
                }
            }
            Info.TablePks = pks;
        }

        // This method is used to retrieve the foreign keys from tabledata to info
        public void GetForeignKeys()
        {
            Info.TableFks = Tabledata.TableFks.Where(f => Info.TablesSelected.Contains(f.Table) || Info.TablesSelected.Contains(f.Table2)).ToArray();
        }

        // This method retrieves the primary key(s) of a table (all or only the non spacial ones)
        private string[] GetPrimaryKey(string table, bool getSpacial)
        {
            string[] data = Tabledata.TablePks[table];
            if (getSpacial)
            {
                return data;
            }
            return FilterSpacial(table, data);
        }

        // This method gathers the primary key(s) of a table from the database
        private string[] FindPrimaryKey(string table)
        {
            string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\getPrimaryKey.sql");
            string sql = System.IO.File.ReadAllText(path);
            DataSet ds = Broker.Instance().Run(new SqlCommand(sql + table + "'", Con), "schema");
            DataTable dt = ds.Tables["schema"];
            string[] res = new string[dt.Rows.Count];

            for (int j = 0; j < dt.Rows.Count; j++)
            {
                res[j] = (string)dt.Rows[j][0];
            }
            Array.Sort(res);

            return res;
        }

        // This method is used to retrieve the suggested primary keys from tabledata to info
        public void GetSuggestedKeys()
        {
            Dictionary<string, string[]> suggested_pks = new Dictionary<string, string[]>();

            foreach (string entry in Info.TablesSelected)
            {
                suggested_pks.Add(entry, Tabledata.TableSuggestedPks[entry]);
            }
            Info.TableSuggestedPks = suggested_pks;
        }
        // This method is used to retrieve the suggested foreign keys from tabledata to info
        public void GetSuggestedFks()
        {
            Info.TableSuggestedFks = Tabledata.TableSuggestedFks.Where(f => Info.TablesSelected.Contains(f.Table) || Info.TablesSelected.Contains(f.Table2)).ToArray();
        }

        // This method applies finds a suitable value for the primary key of a table
        private string[] FindSuggestedPks(string table)
        {
            string res = "";
            bool found = false;
            int count = 0;
            int distinct, total;

            string[] columns = GetTableColumns(table, true);
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
            string[] combinations = GetCombinations(aux);
            while (!found && count < combinations.Length)
            {
                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT COUNT(*) FROM (SELECT DISTINCT " + combinations[count] + " FROM " + GetTableSchemaName(table) + ") AS internalQuery", Con), "distinct");
                distinct = (int)ds.Tables["distinct"].Rows[0][0];
                ds = Broker.Instance().Run(new SqlCommand("SELECT COUNT(*) FROM (SELECT " + combinations[count] + " FROM " + GetTableSchemaName(table) + ") AS internalQuery", Con), "total");
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
                combinations = GetCombinations(columns);
            }
            while (!found && count < combinations.Length)
            {
                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT COUNT(*) FROM (SELECT DISTINCT " + combinations[count] + " FROM " + GetTableSchemaName(table) + ") AS internalQuery", Con), "distinct");
                distinct = (int)ds.Tables["distinct"].Rows[0][0];
                ds = Broker.Instance().Run(new SqlCommand("SELECT COUNT(*) FROM (SELECT " + combinations[count] + " FROM " + GetTableSchemaName(table) + ") AS internalQuery", Con), "total");
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

        // This method applies finds suitables values for the foreign keys of the database tables
        private Models.Constraint[] FindSuggestedFks()
        {
            List<Models.Constraint> res = new List<Models.Constraint>();
            string[] tables = Tabledata.Tables;
            for (int i = 0; i < tables.Length; i++)
            {
                string table = tables[i];
                for (int j = 0; j < tables.Length; j++)
                {
                    string table2 = tables[j];
                    if (table != table2)
                    {
                        string[] tableColumns = Tabledata.TablesColumns[table];
                        string[] table2Columns = Tabledata.TablesColumns[table2];
                        foreach (string column in tableColumns)
                        {
                            if (table2Columns.Contains(column) && Tabledata.TablePks[table2].Length == 1 && Tabledata.TablePks[table2][0] == column)
                            {
                                string name = "FK_" + table + '_' + table2 + '_' + column;
                                res.Add(new Models.Constraint(name, table, table2, "FOREIGN KEY"));
                            }
                        }
                    }
                }
            }
            return res.ToArray();
        }

        // This method calculates and returns all the possible pk combinations ordered
        public static string[] GetCombinations(string[] columns)
        {
            int count = (int)Math.Pow(2, columns.Length);
            string[] res = new string[count - 1];
            for (int i = 1; i < count; i++)
            {
                string str = Convert.ToString(i, 2).PadLeft(columns.Length, '0');

                StringBuilder bld = new StringBuilder();
                for (int j = 0; j < str.Length; j++)
                {
                    if (str[j] == '1')
                    {
                        bld.Append(columns[j]);
                        bld.Append(',');
                    }
                }
                string combination = bld.ToString();
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
        public void GetPrimaryKeysRecords()
        {
            foreach (KeyValuePair<string, string[]> entry in Info.ColumnsSelected)
            {
                string[] pks = GetPrimaryKey(entry.Key, false);
                foreach (string pk in pks)
                {
                    string name = entry.Key + '.' + pk;
                    if (!Info.Records.ContainsKey(name))
                    {
                        GetRecord(name);
                    }
                }
                if (!Info.TablePks.ContainsKey(entry.Key))
                {
                    Info.TablePks.Add(entry.Key, pks);
                }
            }

        }

        // This method retrieves the already loaded data from the selected columns
        public void GetRecord(string record)
        {
            Info.Records.Add(record, Tabledata.Records[record]);
        }

        // This method finds the duplicated data from the selected columns and returns it
        public void GetDuplicates()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            foreach (KeyValuePair<string, string[]> entry in Info.ColumnsSelected)
            {
                string[] columns = GetTableColumns(entry.Key, false);
                columns = ArrayToString(columns, true).Split(',');
                columns = columns.Select(x => "a." + x).ToArray();
                string[] SELcolumns = ArrayToString(entry.Value, true).Split(',').Select(x => "b." + x).ToArray();
                StringBuilder bld = new StringBuilder();
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    int index = Array.IndexOf(GetTableColumns(entry.Key, false), entry.Value[i]);
                    if (i == 0)
                    {
                        bld.Append(columns[index] + " = " + SELcolumns[i]);
                    }
                    else
                    {
                        bld.Append(" AND " + columns[index] + " = " + SELcolumns[i]);
                    }
                }
                string joins = bld.ToString();
                DataSet ds = Broker.Instance().Run(
                new SqlCommand("SELECT " + ArrayToString(columns, false) + " FROM " + GetTableSchemaName(entry.Key)
                + " a JOIN ( SELECT " + ArrayToString(entry.Value, true) + " FROM " + GetTableSchemaName(entry.Key)
                + " GROUP BY " + ArrayToString(entry.Value, true) + " HAVING COUNT(*)>1) b ON " + joins
                + " ORDER BY " + ArrayToString(entry.Value, true), Con), "duplicates");
                DataTable dt = ds.Tables["duplicates"];
                string[][] container = new string[columns.Length][];
                if (dt != null)
                {
                    for (int x = 0; x < dt.Rows.Count; x++)
                    {
                        for (int i = 0; i < columns.Length; i++)
                        {
                            if (x == 0)
                            {
                                container[i] = new string[dt.Rows.Count];
                            }
                            container[i][x] = dt.Rows[x][i].ToString();
                        }
                    }
                }
                columns = GetTableColumns(entry.Key, false);
                for (int i = 0; i < columns.Length; i++)
                {
                    string name = entry.Key + '.' + columns[i];
                    if (container[i] != null)
                    {
                        res.Add(name, container[i]);
                    }
                }
                Info.ColumnsSelected[entry.Key] = columns;
            }
            Info.Records = res;
        }

        // This method retrieves the already found missing values from the selected columns
        public void GetMissingValue(string missingValue)
        {
            string table = missingValue.Split('.')[0];
            string[] columns = GetTableColumns(table, false);
            for (int i = 0; i < columns.Length; i++)
            {
                string name = table + '.' + columns[i];
                Info.Records.Add(name, Tabledata.MissingValues[name]);
            }
        }

        // This method finds the missing values from the selected columns and saves them
        public void FindMissingValues()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            foreach (KeyValuePair<string, string[]> entry in Info.ColumnsSelected)
            {
                string[] columns = GetTableColumns(entry.Key, false);
                StringBuilder bld = new StringBuilder();
                bld.Append(entry.Value[0] + " IS NULL");
                for (int i = 1; i < entry.Value.Length; i++)
                {
                    bld.Append(" OR " + entry.Value[i] + " IS NULL");
                }
                DataSet ds = Broker.Instance().Run(
                    new SqlCommand("SELECT " + ArrayToString(columns, false) + " FROM " + GetTableSchemaName(entry.Key)
                    + " WHERE " + bld.ToString(), Con), "missing");
                DataTable dt = ds.Tables["missing"];
                string[][] container = new string[columns.Length][];
                if (dt != null && dt.Rows.Count > 0)
                {
                    for (int x = 0; x < dt.Rows.Count; x++)
                    {
                        for (int i = 0; i < columns.Length; i++)
                        {
                            if (x == 0)
                            {
                                container[i] = new string[dt.Rows.Count];
                            }
                            container[i][x] = dt.Rows[x][i].ToString();
                        }
                    }
                    for (int i = 0; i < columns.Length; i++)
                    {
                        string name = entry.Key + '.' + columns[i];
                        if (container[i] != null)
                        {
                            res.Add(name, container[i]);
                        }
                        else
                        {
                            res.Add(name, Array.Empty<string>());
                        }
                    }
                    string[] other = new string[columns.Length + entry.Value.Length];
                    Array.Copy(columns, 0, other, 0, columns.Length);
                    string[] aux = entry.Value.Select(s => s + "Missing").ToArray();
                    Array.Copy(aux, 0, other, columns.Length, aux.Length);
                    Info.ColumnsSelected[entry.Key] = other;
                }
                else
                {
                    Info.ColumnsSelected.Remove(entry.Key);
                }
            }
            Tabledata.MissingValues = res;
        }

        // This method gets the data of the columns from the database
        private void FindRecords()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            foreach (KeyValuePair<string, string[]> entry in GetColumns(false))
            {
                string columnsNames = ArrayToString(entry.Value, true);

                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT " + columnsNames + " FROM " + GetTableSchemaName(entry.Key), Con), "records");
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
            Tabledata.Records = res;
        }

        // This method gathers the names of the tables and their columns from the database and saves the result to the corresponding Model variable
        private void FindTableAndColumnData()
        {
            string[] tables = Tabledata.Tables;
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            for (int i = 0; i < tables.Length; i++)
            {
                res.Add(tables[i], FindTableColumns(tables[i]));
            }

            Dictionary<string, string[]> result = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string[]> entry in res.OrderBy(key => key.Key))
            {
                result.Add(entry.Key, entry.Value);
            }

            Tabledata.TablesColumns = result;
        }

        // This method gathers the names of the tables from the database and saves the result to the corresponding Model variable
        private void FindTableData()
        {
            DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'", Con), "tables");
            DataTable dt = ds.Tables["tables"];
            DataRow rows;
            string[] res = new string[dt.Rows.Count];

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                rows = dt.Rows[i];
                res[i] = (string)rows[0];
            }

            Array.Sort<string>(res);

            Tabledata.Tables = res;
        }

        // This method gathers the names of the columns of a given table from the database and returns the result
        public Dictionary<string, string[]> GetColumns(bool getSpacial)
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            foreach (string table in Tabledata.Tables)
            {
                res.Add(table, GetTableColumns(table, getSpacial));
            }
            return res;
        }

        // This method gathers the names of the columns of a given table from the database and returns the result
        private string[] GetTableColumns(string table, bool getSpacial)
        {
            string[] res = Tabledata.TablesColumns[table];

            if (!getSpacial)
            {
                res = FilterSpacial(table, res);
            }
            return res;
        }

        // This method gathers the names of the columns of from the database and returns the result
        private string[] FindTableColumns(string table)
        {
            DataSet dsC;
            DataTable dtC;

            dsC = Broker.Instance().Run(new SqlCommand("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "'", Con), "columns");
            dtC = dsC.Tables["columns"];

            string[] aux = new string[dtC.Rows.Count];

            for (int j = 0; j < dtC.Rows.Count; j++)
            {
                aux[j] = (string)dtC.Rows[j][0];
            }

            return aux;
        }

        private void FindConstraints()
        {
            List<Models.Constraint> res = new List<Models.Constraint>();

            string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\findForeign.sql");
            string sql = System.IO.File.ReadAllText(path);
            DataSet ds = Broker.Instance().Run(new SqlCommand(sql, Con), "findConstraints");
            DataTable dt = ds.Tables["findConstraints"];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string name = (string)dt.Rows[i][1];
                string table = (string)dt.Rows[i][0];
                string table2 = (string)dt.Rows[i][2];
                Models.Constraint aux = new Models.Constraint(name, table, table2, "FOREIGN KEY");
                res.Add(aux);
            }

            path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\findPrimary.sql");
            sql = System.IO.File.ReadAllText(path);
            ds = Broker.Instance().Run(new SqlCommand(sql, Con), "findConstraints");
            dt = ds.Tables["findConstraints"];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string name = (string)dt.Rows[i][1];
                string table = (string)dt.Rows[i][0];
                Models.Constraint aux = new Models.Constraint(name, table, "PRIMARY KEY");
                res.Add(aux);
            }

            path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\findComputed.sql");
            sql = System.IO.File.ReadAllText(path);
            ds = Broker.Instance().Run(new SqlCommand(sql, Con), "findConstraints");
            dt = ds.Tables["findConstraints"];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string column = (string)dt.Rows[i][1];
                string table = (string)dt.Rows[i][0];
                string definition = (string)dt.Rows[i][2];
                Models.Constraint aux = new Models.Constraint(column, table, definition, column);
                res.Add(aux);
            }

            path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\findIndexes.sql");
            sql = System.IO.File.ReadAllText(path);
            ds = Broker.Instance().Run(new SqlCommand(sql, Con), "findConstraints");
            dt = ds.Tables["findConstraints"];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string name = (string)dt.Rows[i][1];
                string table = (string)dt.Rows[i][0];
                string column = name.Split('_', 3)[2];
                Models.Constraint aux = new Models.Constraint(name, table, column, "INDEX");
                res.Add(aux);
            }

            Tabledata.Constraints = res.ToArray();
        }

        private void DeleteConstraints(string table)
        {
            foreach (Models.Constraint c in Tabledata.Constraints.Where(s => s.Type != "COMPUTED COLUMN" && s.Type != "INDEX" && (s.Table == table || s.Table2 == table)))
            {
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + c.Table + " DROP CONSTRAINT " + c.Name, Con), "findConstraints");
            }
            foreach (Models.Constraint c in Tabledata.Constraints.Where(s => s.Type == "COMPUTED COLUMN" && s.Table == table))
            {
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + c.Table + " DROP COLUMN " + c.Column, Con), "findConstraints");
            }
            foreach (Models.Constraint c in Tabledata.Constraints.Where(s => s.Type == "INDEX" && s.Table == table))
            {
                Broker.Instance().Run(new SqlCommand("DROP INDEX " + c.Name + " ON " + c.Table, Con), "findConstraints");
            }
        }

        private void ReplaceConstraints(string table)
        {
            foreach (Models.Constraint c in Tabledata.Constraints.Where(s => s.Type == "COMPUTED COLUMN" && s.Table == table))
            {
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + c.Table + " ADD " + c.Column + " AS " + c.Definition, Con), "findConstraints");
            }
            foreach (Models.Constraint c in Tabledata.Constraints.Where(s => s.Type == "PRIMARY KEY" && s.Table == table))
            {
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + c.Table + " ADD CONSTRAINT " + c.Name + " PRIMARY KEY(" + c.Column + ")", Con), "findConstraints");
            }
            foreach (Models.Constraint c in Tabledata.Constraints.Where(s => s.Type == "FOREIGN KEY" && (s.Table == table || s.Table2 == table)))
            {
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + c.Table + " ADD CONSTRAINT " + c.Name + " FOREIGN KEY(" + c.Column
                    + ") REFERENCES " + c.Table2 + " (" + c.Column + ") ON DELETE CASCADE ON UPDATE CASCADE", Con), "findConstraints");
            }
            foreach (Models.Constraint c in Tabledata.Constraints.Where(s => s.Type == "INDEX" && s.Table == table))
            {
                string aux = ArrayToString(c.Column.Split('_'), true);
                Broker.Instance().Run(new SqlCommand("CREATE INDEX " + c.Name + " ON " + c.Table + " (" + aux + ")", Con), "findConstraints");
            }
        }


        // This method updates the database with the corresponding changes
        public void Update(string data)
        {
            switch (Info.Functionality)
            {
                case "create_masks":
                    SelectMaskedRecords(data);
                    UpdateDataMasking();
                    break;
                case "create_restrictions":
                    UpdateRestrictions(data);
                    break;
                case "primary_keys":
                    SelectPksTables(data);
                    UpdatePrimaryKeys();
                    break;
                case "foreign_keys":
                    SelectFksTables(data);
                    UpdateForeignKeys();
                    break;
                case "remove_duplicates":
                    SelectDuplicates(data);
                    DeleteDuplicates();
                    break;
                case "improve_datatypes":
                    SelectDatatypes(data);
                    UpdateDatatypes();
                    break;
                case "missing_values":
                    UpdateMissingValues(SelectMissingValues(data));
                    break;
                case "improve_indexes":
                    SelectIndexes(data);
                    UpdateIndexes();
                    break;
                case "table_defragmentation":
                    UpdateTableDefrag(data);
                    break;
                case "data_unification":
                    UpdateUnification(data);
                    break;
            }
        }

        // This method updates the database with the corresponding changes for functionality data_masking
        private void UpdateDataMasking()
        {
            foreach (KeyValuePair<string, string[]> entry in Info.ColumnsSelected)
            {
                string[] pks = GetPrimaryKey(entry.Key, false);
                string[][] pk_data = new string[pks.Length][];

                for (int i = 0; i < pks.Length; i++)
                {
                    string pkName = entry.Key + '.' + pks[i];
                    pk_data[i] = Tabledata.Records[pkName];
                }

                foreach (string column in entry.Value)
                {
                    string name = entry.Key + '.' + column;
                    string aux = name + "Masked";
                    string[] data_masked = Tabledata.Records[aux];
                    string[] data = Tabledata.Records[name];

                    for (int i = 0; i < data.Length; i++)
                    {
                        if (data[i] != data_masked[i])
                        {
                            StringBuilder bld = new StringBuilder();
                            bld.Append(" WHERE");

                            for (int j = 0; j < pks.Length; j++)
                            {
                                string pkName = entry.Key + '.' + pks[j];
                                string type = GetDatatype(pkName);
                                bld.Append(" " + pks[j] + " = convert(" + type + ", '" + pk_data[j][i]);
                                if (type == "datetime")
                                {
                                    bld.Append("', 103)");
                                }
                                else
                                {
                                    bld.Append("')");
                                }
                                if (j != pks.Length - 1)
                                {
                                    bld.Append(" and");
                                }
                            }
                            if (pks.Length == 0)
                            {
                                string type = GetDatatype(name);
                                bld.Append(" " + column + " = convert(" + type + ", '" + data[i]);
                                if (type == "datetime")
                                {
                                    bld.Append("', 103)");
                                }
                                else
                                {
                                    bld.Append("')");
                                }
                            }
                            Broker.Instance().Run(new SqlCommand("UPDATE " + GetTableSchemaName(entry.Key) + " SET " + column + " = '" + data_masked[i] + "'" + bld.ToString(), Con), "update");
                        }
                    }
                    Tabledata.Records[name] = data_masked;
                    Tabledata.Records.Remove(aux);
                }
            }
        }

        // This method updates the database with the corresponding changes for functionality primary_keys
        private void UpdatePrimaryKeys()
        {
            foreach (string entry in Info.TablesSelected)
            {
                StringBuilder bld = new StringBuilder();
                if (Tabledata.TablePks[entry].Length != 0)
                {
                    bld.Append(GetPKConstraint(entry));
                    Broker.Instance().Run(new SqlCommand("ALTER TABLE " + GetTableSchemaName(entry) + " DROP CONSTRAINT " + bld.ToString(), Con), "dropPK");
                }
                bld = new StringBuilder();
                bld.Append("pk_" + entry);
                foreach (string column in Tabledata.TableSuggestedPks[entry])
                {
                    MakeNotNull(column, entry);
                    bld.Append('_');
                    bld.Append(column);
                }
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + GetTableSchemaName(entry) + " ADD CONSTRAINT " + bld.ToString() + " PRIMARY KEY (" + ArrayToString(Tabledata.TableSuggestedPks[entry], true) + ")", Con), "addPK");
                Tabledata.TablePks[entry] = Tabledata.TableSuggestedPks[entry];
            }
        }

        // This method updates the database with the corresponding changes for functionality foreign_keys
        private void UpdateForeignKeys()
        {
            foreach (Models.Constraint c in Info.TableFks)
            {
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + GetTableSchemaName(c.Table) + " DROP CONSTRAINT " + c.Name, Con), "findConstraints");
            }
            foreach (Models.Constraint c in Info.TableSuggestedFks)
            {
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + GetTableSchemaName(c.Table) + " ADD CONSTRAINT " + c.Name + " FOREIGN KEY(" + c.Column
                    + ") REFERENCES " + GetTableSchemaName(c.Table2) + " (" + c.Column + ")", Con), "findConstraints");
            }
            FindFks();
        }

        // This method updates the database with the corresponding changes for functionality remove_duplicates
        private void DeleteDuplicates()
        {
            foreach (KeyValuePair<string, string[]> entry in Info.ColumnsSelected)
            {
                string[] pks = GetPrimaryKey(entry.Key, false);
                if (pks.Length != 0)
                {
                    DeleteConstraints(GetTableSchemaName(entry.Key));
                    string name1 = entry.Key + '.' + pks[0];
                    for (int j = 0; j < Info.Records[name1].Length; j++)
                    {
                        StringBuilder bld = new StringBuilder();

                        for (int i = 0; i < pks.Length; i++)
                        {
                            string name = entry.Key + '.' + pks[i];
                            if (i == 0)
                            {
                                bld.Append(pks[i] + " = " + Info.Records[name][j]);
                            }
                            else
                            {
                                bld.Append(" AND " + pks[i] + " = " + Info.Records[name][j]);
                            }
                        }

                        Broker.Instance().Run(new SqlCommand("DELETE FROM " + GetTableSchemaName(entry.Key) + " WHERE " + bld.ToString(), Con), "removeDuplicates");
                    }
                    ReplaceConstraints(GetTableSchemaName(entry.Key));
                }
            }
            FindRecords();
        }

        // This method updates the database with the corresponding changes for functionality improve_datatypes
        private void UpdateDatatypes()
        {
            foreach (KeyValuePair<string, string[]> entry in Info.ColumnsSelected)
            {
                DeleteConstraints(GetTableSchemaName(entry.Key));
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name = entry.Key + '.' + entry.Value[i];
                    Broker.Instance().Run(new SqlCommand("ALTER TABLE " + GetTableSchemaName(entry.Key) + " ALTER COLUMN " + entry.Value[i] + " " + Info.ColumnsSuggestedDatatypes[name] + " NOT NULL", Con), "datatypeChange");
                }
                ReplaceConstraints(GetTableSchemaName(entry.Key));
            }
            FindDatatypes();
        }

        // This method updates the database with the corresponding changes for functionality missing_values
        private void UpdateMissingValues(string mode)
        {
            foreach (KeyValuePair<string, string[]> entry in Info.ColumnsSelected)
            {
                DeleteConstraints(GetTableSchemaName(entry.Key));
                StringBuilder bld = new StringBuilder();
                string[] pks = Tabledata.TablePks[entry.Key];
                switch (mode)
                {
                    case "deleteColumns":
                        bld.Append("ALTER TABLE " + GetTableSchemaName(entry.Key) + " DROP COLUMN " + entry.Value[0]);
                        for (int i = 1; i < entry.Value.Length; i++)
                        {
                            bld.Append("," + entry.Value[i]);
                        }
                        Broker.Instance().Run(new SqlCommand(bld.ToString(), Con), "missingValues");
                        FindTableAndColumnData();
                        FindFks();
                        FindUnification();
                        break;
                    case "updateRows":
                        StringBuilder bldUpdate = new StringBuilder();
                        string name = entry.Key + '.' + entry.Value[0];
                        int lth = Info.Records[name].Length;
                        for (int j = 0; j < lth; j++)
                        {
                            for (int k = 0; k < pks.Length; k++)
                            {
                                string pkname = entry.Key + '.' + pks[k];
                                if (k == 0)
                                {
                                    bld.Append(pks[k] + " = " + Info.Records[pkname][j]);
                                }
                                else
                                {
                                    bld.Append(" AND " + pks[k] + " = " + Info.Records[pkname][j]);
                                }
                            }
                            for (int k = 0; k < entry.Value.Length; k++)
                            {
                                name = entry.Key + '.' + entry.Value[k];
                                if (Info.ColumnsSelected[entry.Key].Contains(@entry.Value[k] + "Missing"))
                                {
                                    if (k == 0 || bldUpdate.ToString() == "")
                                    {
                                        bldUpdate.Append(entry.Value[k] + " = ");
                                        if (!int.TryParse(Info.Records[name][j], out _))
                                        {
                                            bldUpdate.Append('\'');
                                        }
                                        bldUpdate.Append(Info.Records[name][j]);
                                        if (!int.TryParse(Info.Records[name][j], out _))
                                        {
                                            bldUpdate.Append('\'');
                                        }
                                    }
                                    else
                                    {
                                        bldUpdate.Append(" AND " + entry.Value[k] + " = ");
                                        if (!int.TryParse(Info.Records[name][j], out _))
                                        {
                                            bldUpdate.Append('\'');
                                        }
                                        bldUpdate.Append(Info.Records[name][j]);
                                        if (!int.TryParse(Info.Records[name][j], out _))
                                        {
                                            bldUpdate.Append('\'');
                                        }
                                    }

                                }
                            }
                            Broker.Instance().Run(new SqlCommand("UPDATE " + GetTableSchemaName(entry.Key) + " SET " + bldUpdate.ToString() + " WHERE " + bld.ToString(), Con), "missingValues");
                        }
                        FindMissingValues();
                        FindRecords();
                        break;
                    case "deleteRows":
                        name = entry.Key + '.' + entry.Value[0];
                        for (int j = 0; j < Info.Records[name].Length; j++)
                        {
                            for (int k = 0; k < pks.Length; k++)
                            {
                                string pkname = entry.Key + '.' + pks[k];
                                if (k == 0)
                                {
                                    bld.Append(pks[k] + " = " + Info.Records[pkname][j]);
                                }
                                else
                                {
                                    bld.Append(" AND " + pks[k] + " = " + Info.Records[pkname][j]);
                                }
                            }
                            Broker.Instance().Run(new SqlCommand("DELETE FROM " + GetTableSchemaName(entry.Key) + " WHERE " + bld.ToString(), Con), "missingValues");
                        }
                        FindMissingValues();
                        FindRecords();
                        break;
                }
                ReplaceConstraints(GetTableSchemaName(entry.Key));
            }
        }

        // This method updates the database with the corresponding changes for functionality missing_values
        private void UpdateIndexes()
        {
            foreach (KeyValuePair<string, string[]> entry in Info.Records)
            {
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    Broker.Instance().Run(new SqlCommand(entry.Value[i], Con), "updateIndexes");
                }
            }
            FindIndexes();
        }

        // This method updates the database with the corresponding changes for functionality table_defragmentation
        private void UpdateTableDefrag(string data)
        {
            string[] aux = data.Split(',');
            string[] tables = new string[aux.Length - 1];
            Array.Copy(aux, 1, tables, 0, aux.Length - 1);
            Info.TablesSelected = tables;

            foreach (string table in Info.TablesSelected)
            {
                Broker.Instance().Run(new SqlCommand("DBCC DBREINDEX('" + GetTableSchemaName(table) + "') WITH NO_INFOMSGS", Con), "updateTableDefrag");
            }
        }

        // This method updates the database with the corresponding changes for functionality data_unification
        private void UpdateUnification(string data)
        {
            Dictionary<string, int[]> input = new Dictionary<string, int[]>();
            foreach (string a in data.Split('/'))
            {
                if (a != "undefined")
                {
                    string table = a.Split(',')[0];
                    string aux = "";
                    string[] splitted = a.Split(',');
                    List<string> recordsList = new List<string>();
                    for (int j = 1; j < splitted.Length; j++)
                    {
                        foreach (string item in splitted[j].Split('_'))
                        {
                            recordsList.Add(item);
                        }
                    }
                    string[] records = recordsList.ToArray();
                    List<int> values = new List<int>();
                    for (int i = 0; i < records.Length; i += 2)
                    {
                        string column = records[i];
                        string name = table + '.' + aux;
                        int record = Int32.Parse(records[i + 1]);
                        if (aux != "" && aux != column)
                        {
                            input.Add(name, values.ToArray());
                            values = new List<int>();
                        }
                        aux = column;
                        values.Add(record);
                        if (i == records.Length - 2)
                        {
                            name = table + '.' + column;
                            input.Add(name, values.ToArray());
                        }
                    }

                }
            }

            foreach (KeyValuePair<string, int[]> entry in input)
            {
                string table = GetTableSchemaName(entry.Key.Split('.')[0]);
                string column = entry.Key.Split('.')[1];
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string newValue = Info.Records[entry.Key][entry.Value[i]];
                    string oldValue;
                    if (entry.Value[i] % 2 == 0)
                    {
                        oldValue = Info.Records[entry.Key][entry.Value[i] + 1];
                    }
                    else
                    {
                        oldValue = Info.Records[entry.Key][entry.Value[i] - 1];
                    }
                    Broker.Instance().Run(new SqlCommand("UPDATE " + table + " SET " + column + " = '" + newValue + "' WHERE " + column + " = '" + oldValue + "'", Con), "updateUnification");
                }
            }
            FindRecords();
            FindUnification();
        }

        // This method updates the database with the corresponding changes for functionality restrictions
        private void UpdateRestrictions(string data)
        {
            Dictionary<int, int[]> input = new Dictionary<int, int[]>();
            foreach (string a in data.Split('/'))
            {
                if (a != "undefined")
                {
                    int index = Int32.Parse(a.Split(',')[0]);
                    string[] splitted = a.Split(',');
                    List<int> recordsList = new List<int>();
                    for (int j = 1; j < splitted.Length; j++)
                    {
                        foreach (string item in splitted[j].Split('_'))
                        {
                            recordsList.Add(Int32.Parse(item));
                        }
                    }
                    input.Add(index, recordsList.ToArray());
                }
            }

            foreach (KeyValuePair<int, int[]> entry in input)
            {
                Restriction r = Info.Restrictions[entry.Key];
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name1 = r.Table + entry.Key + '.' + r.Column1;
                    string name2 = r.Table + entry.Key + '.' + r.Column2;

                    Broker.Instance().Run(new SqlCommand("UPDATE " + GetTableSchemaName(r.Table) + " SET " + r.Column2 + " = '" + Info.Records[name2][entry.Value[i]] + "' WHERE " + r.Column1 + " = '" + Info.Records[name1][entry.Value[i]] + "'", Con), "updateRestrictions");
                }
            }
            FindRecords();
        }

        // This method is used to retrieve the already computed available masks for a column
        public void GetAvailableMasks()
        {
            Dictionary<string, bool[]> res = new Dictionary<string, bool[]>();
            Dictionary<string, string[]> selection = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string[]> entry in Info.ColumnsSelected)
            {
                string[] aux = new string[entry.Value.Length];
                int index = 0;

                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name = entry.Key + '.' + entry.Value[i];

                    if (Tabledata.MasksAvailable.ContainsKey(name))
                    {
                        res.Add(name, Tabledata.MasksAvailable[name]);
                        aux[index] = entry.Value[i];
                        index++;
                    }
                }
                if (index != 0)
                {
                    string[] other = new string[index];
                    Array.Copy(aux, 0, other, 0, index);
                    selection.Add(entry.Key, other);
                }
            }
            Info.MasksAvailable = res;
            Info.ColumnsSelected = selection;
        }

        // This method is used to filter the masks available to each column according to the data stored in it
        private void FindAvailableMasks()
        {
            Dictionary<string, bool[]> auxDict = new Dictionary<string, bool[]>();

            foreach (KeyValuePair<string, string[]> entry in GetColumns(false))
            {
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    bool[] res = new bool[4];
                    float[] comply = new float[4];
                    string name = entry.Key + '.' + entry.Value[i];
                    string[] data = Tabledata.Records[name];

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

                            if (IsDNI(value))
                            {
                                comply[0]++;
                            }

                            if (IsEmail(value))
                            {
                                comply[1]++;
                            }

                            if (IsPhone(value))
                            {
                                comply[2]++;
                            }

                            if (IsCCN(value))
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
            Tabledata.MasksAvailable = auxDict;
        }

        // This method is used to retrieve the values that don't follow the selected restrictions for restrictions functionality
        public void GetRestrictions()
        {
            Info.Restrictions = Info.Restrictions.OrderBy(r => r.Table).ToList();
            int index = 0;
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            foreach (Restriction r in Info.Restrictions)
            {
                string name1 = r.Table + index + '.' + r.Column1;
                string name2 = r.Table + index + '.' + r.Column2;
                List<string> list1 = new List<string>();
                List<string> list2 = new List<string>();
                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT DISTINCT a.[" + r.Column1 + "],a.[" + r.Column2 + "] FROM " + GetTableSchemaName(r.Table)
                    + " a JOIN(SELECT [" + r.Column1 + "], [" + r.Column2 + "] FROM " + GetTableSchemaName(r.Table) + " GROUP BY [" + r.Column1 + "], [" + r.Column2
                    + "] HAVING COUNT(*)> 1) b ON a.[" + r.Column1 + "] = b.[" + r.Column1 + "] ORDER BY [" + r.Column1 + "],[" + r.Column2 + "]", Con), "getRestriction");
                DataTable dt = ds.Tables["getRestriction"];

                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    bool entered = false;

                    if (list1.Count == 0 || !(i == dt.Rows.Count - 1 && (string)dt.Rows[i][0] != list1[^1]))
                    {
                        list1.Add(dt.Rows[i][0].ToString());
                        list2.Add(dt.Rows[i][1].ToString());
                    }
                    else if (list1.Count == 1 || list1[^1] != list1[^2])
                    {
                        list1.RemoveAt(index: list1.Count - 1);
                        list2.RemoveAt(index: list2.Count - 1);
                        entered = true;
                    }
                    if ((list1.Count == 2) && (dt.Rows[i][0].ToString() != list1[0]))
                    {
                        list1.RemoveAt(0);
                        list2.RemoveAt(0);
                    }
                    else if (list1.Count > 2 && !entered && dt.Rows[i][0].ToString() != list1[^2] && list1[^2] != list1[^3])
                    {
                        list1.RemoveAt(list1.Count - 2);
                        list2.RemoveAt(list2.Count - 2);
                    }
                }
                if (list1.Count > 1)
                {
                    res.Add(name1, list1.ToArray());
                    res.Add(name2, list2.ToArray());
                }
                index++;
            }
            Info.Records = res;
        }

        // This method is used to gather all information needed pre initialization
        public void InitMetatable()
        {
            FindTableData();
            FindTableAndColumnData();
            FindTablesSchemaNames();
            FindDatatypes();
            FindRecords();
            FindSuggestedDatatypes();
            FindAvailableMasks();
            FindPks();
            FindConstraints();
            FindFks();
            FindIndexes();
            FindUnification();
            this.Perf = GetPerformance();
        }

        // This method is used to gather performance initial status
        public Performance GetPerformance()
        {
            Performance res = new Performance(Tabledata.Database);
            foreach (string tableI in Tabledata.Tables)
            {
                string table = GetTableSchemaName(tableI);
                Int64 qt;
                string query_time;
                DataSet ds, ds2;
                ds = Broker.Instance().Run(new SqlCommand("select ms_ticks from sys.dm_os_sys_info", Con), "initPerformance");
                for (int i = 0; i < 10; i++)
                {
                    Broker.Instance().Run(new SqlCommand("SELECT * FROM " + table, Con), "initPerformance");
                }
                ds2 = Broker.Instance().Run(new SqlCommand("select ms_ticks from sys.dm_os_sys_info", Con), "initPerformance");
                DataTable dt = ds.Tables["initPerformance"];
                qt = (Int64)dt.Rows[0][0];
                dt = ds2.Tables["initPerformance"];
                qt = (Int64)dt.Rows[0][0] - qt;
                qt /= 10;
                if (qt == 0)
                {
                    qt++;
                }
                query_time = qt.ToString() + " ms";
                ds = Broker.Instance().Run(new SqlCommand("exec sp_spaceused '" + table + "'", Con), "initPerformance");
                dt = ds.Tables["initPerformance"];
                res.InsertFirst(table, dt.Rows[0][1].ToString(), dt.Rows[0][2].ToString(), dt.Rows[0][3].ToString(), dt.Rows[0][4].ToString(), dt.Rows[0][5].ToString(), query_time);
            }
            return res;
        }

        // This method is used to recalculate the performance view values
        public void UpdatePerformance()
        {
            Performance p = GetPerformance();
            foreach (string tableI in Tabledata.Tables)
            {
                string table = GetTableSchemaName(tableI);
                Perf.InsertLater(table, p.Rows[table][0], p.Reserved[table][0], p.Data[table][0], p.Index_size[table][0], p.Unused[table][0], p.Query_time[table][0]);
            }
            Perf.Log = Tabledata.Log;
        }

        // This method gathers the schema names of the tables from the database
        private void FindTablesSchemaNames()
        {
            Dictionary<string, string> res = new Dictionary<string, string>();

            foreach (string table in Tabledata.Tables)
            {
                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT '[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '" + table + "'", Con), "schema");
                DataTable dt = ds.Tables["schema"];
                DataRow row = dt.Rows[0];
                res.Add(table, (string)row[0]);
            }

            Tabledata.TablesSchemaNames = res;

        }

        // This method gathers the schema name of a table from the database
        private string GetTableSchemaName(string table)
        {
            return Tabledata.TablesSchemaNames[table];
        }

        // This method gathers the missing and unused indexes of the tables and columns from the database
        private void FindIndexes()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            Dictionary<string, List<string>> aux = new Dictionary<string, List<string>>();

            string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\findUnusedIndexes.sql");
            string sql = System.IO.File.ReadAllText(path);
            DataSet ds = Broker.Instance().Run(new SqlCommand(sql, Con), "findUnusedIndexes");
            DataTable dt = ds.Tables["findUnusedIndexes"];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string table = (string)dt.Rows[i][0];
                string index = (string)dt.Rows[i][1];
                if (!aux.ContainsKey(table))
                {
                    aux[table] = new List<string>();
                }
                aux[table].Add(index);
            }
            path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\findMissingIndexes.sql");
            sql = System.IO.File.ReadAllText(path);
            ds = Broker.Instance().Run(new SqlCommand(sql, Con), "findMissingIndexes");
            dt = ds.Tables["findMissingIndexes"];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string table = ((string)dt.Rows[i][0]).Split('.')[1].Replace("[", "").Replace("]", "");
                string index = (string)dt.Rows[i][1];
                if (!aux.ContainsKey(table))
                {
                    aux[table] = new List<string>();
                }
                aux[table].Add(index);
            }
            foreach (KeyValuePair<string, List<string>> entry in aux)
            {
                res.Add(entry.Key, entry.Value.ToArray());
            }
            Tabledata.Indexes = res;
        }

        // This method gathers the probable missinputted values of the tables and columns from the database
        private void FindUnification()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            const int MAX_N_CHARS = 10;
            const int MIN_N_CHARS = 3;
            List<string> all_pks = new List<string>();
            foreach (KeyValuePair<string, string[]> entry in Tabledata.TablePks)
            {
                foreach (string pk in entry.Value)
                {
                    all_pks.Add(pk);
                }
            }

            foreach (KeyValuePair<string, string[]> entry in Tabledata.TablesColumns)
            {
                string[] columns = GetTableColumns(entry.Key, false);
                for (int j = 1; j < columns.Length; j++)
                {
                    string name = entry.Key + '.' + columns[j];
                    if (!columns[j].Contains("Number") && !entry.Key.Contains("Password") && GetDatatype(name).Contains("nvarchar") && !all_pks.Contains(columns[j]) && Tabledata.Records.ContainsKey(name) && Tabledata.Records[name] != null)
                    {
                        string[] records = Tabledata.Records[name];
                        List<string> aux = new List<string>();
                        records = records.Where(r => r.Length <= MAX_N_CHARS).Where(r => r.Length >= MIN_N_CHARS).Distinct().ToArray();
                        if (records != null && records.Length > 0)
                        {
                            Parallel.ForEach(records, a =>
                            {
                                for (int k = records.ToList().IndexOf(a) + 1; k < records.Length; k++)
                                {
                                    string b = records[k];
                                    if (a != b && StringSimilar(a, b))
                                    {
                                        lock (aux)
                                        {
                                            aux.Add(a);
                                            aux.Add(b);
                                        }
                                    }
                                }
                            });
                            if (aux != null && aux.Count > 0)
                            {
                                res.Add(name, aux.ToArray());
                            }
                        }
                    }
                }
            }
            Tabledata.Unification = res;
        }

        // This method gathers the datatypes of the columns from the database
        private void FindDatatypes()
        {
            Dictionary<string, string> res = new Dictionary<string, string>();

            foreach (KeyValuePair<string, string[]> entry in GetColumns(true))
            {
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name = entry.Key + '.' + entry.Value[i];
                    DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT DATA_TYPE, character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '" + entry.Value[i] + "' AND TABLE_NAME = '" + entry.Key + "'", Con), "type");
                    DataTable dt = ds.Tables["type"];
                    DataRow row = dt.Rows[0];
                    string value = (string)row[0];
                    if (value.Contains("char"))
                    {
                        value += '(' + row[1].ToString() + ')';
                    }
                    res.Add(name, value);
                }
            }

            Tabledata.ColumnsDatatypes = res;
        }

        // This method gathers the datatypes of the columns from the database
        private void FindSuggestedDatatypes()
        {
            Dictionary<string, string> res = new Dictionary<string, string>();

            foreach (KeyValuePair<string, string[]> entry in GetColumns(true))
            {
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name = entry.Key + '.' + entry.Value[i];
                    string result = "nvarchar(100)";

                    if (Tabledata.Records.ContainsKey(name) && Tabledata.Records[name] != null)
                    {
                        string[] record = Tabledata.Records[name];
                        bool number = record.All(r => r.All(char.IsDigit));
                        if (number)
                        {
                            string[] smth = record.Where(s => s != "").ToArray();
                            string max = "";
                            if (smth.Length > 0)
                            {
                                max = smth.OrderByDescending(s => long.Parse(s)).First();
                            }
                            if (record.All(r => (r == "0" || r == "1")))
                            {
                                result = "bit";
                            }
                            else
                            {
                                if (max.Length == 0 || long.Parse(max) < 255)
                                {
                                    result = "tinyint";
                                }
                                else if (long.Parse(max) < 32768)
                                {
                                    result = "smallint";
                                }
                                else if (long.Parse(max) < 2147483648)
                                {
                                    result = "int";
                                }
                                else
                                {
                                    result = "bigint";
                                }
                            }
                        }
                        else
                        {
                            DateTime dt = new DateTime();
                            if (record.All(r => DateTime.TryParse(r, out dt)))
                            {
                                result = "datetime";
                            }
                            else if (record.All(r => r == "True" || r == "False"))
                            {
                                result = "bit";
                            }
                            else
                            {
                                string max = record.OrderByDescending(s => s.Length).First();
                                int value = max.Length;
                                while (value % 5 != 0)
                                {
                                    value++;
                                }
                                result = "nvarchar(" + value + ')';
                            }
                        }
                    }

                    res.Add(name, result);
                }
            }

            Tabledata.ColumnsSuggestedDatatypes = res;
        }

        // This method retrieves the datatypes information of the selected columns
        public void GetIndexes()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            foreach (string entry in Info.TablesSelected)
            {
                if (Tabledata.Indexes.ContainsKey(entry))
                {
                    res.Add(entry, Tabledata.Indexes[entry]);
                }
            }
            Info.Records = res;
        }

        // This method retrieves the datatypes information of the selected columns
        public void GetDatatypes()
        {
            Dictionary<string, string> res = new Dictionary<string, string>();
            Dictionary<string, string> res_sug = new Dictionary<string, string>();

            foreach (KeyValuePair<string, string[]> entry in Info.ColumnsSelected)
            {
                Models.Constraint[] c = Tabledata.Constraints.Where(s => s.Type == "COMPUTED COLUMN" && s.Table == GetTableSchemaName(entry.Key)).ToArray();
                if (c.Length > 0)
                {
                    string[] aux = c.Select(c => c.Column).ToArray();
                    Info.ColumnsSelected[entry.Key] = entry.Value.Where(c => !aux.Contains(c)).ToArray();
                }
            }
            foreach (KeyValuePair<string, string[]> entry in Info.ColumnsSelected)
            {
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name = entry.Key + '.' + entry.Value[i];
                    res.Add(name, Tabledata.ColumnsDatatypes[name]);
                    res_sug.Add(name, Tabledata.ColumnsSuggestedDatatypes[name]);
                }
            }
            Info.ColumnsDatatypes = res;
            Info.ColumnsSuggestedDatatypes = res_sug;
        }

        // This method retrieves the unification information of the selected columns
        public void GetUnification()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            Dictionary<string, string[]> res2 = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string[]> entry in Info.ColumnsSelected)
            {
                List<string> aux = new List<string>();
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name = entry.Key + '.' + entry.Value[i];
                    if (Tabledata.Unification.ContainsKey(name))
                    {
                        res.Add(name, Tabledata.Unification[name]);
                        aux.Add(entry.Value[i]);
                    }
                }
                if (aux.Count > 0)
                {
                    res2.Add(entry.Key, aux.ToArray());
                }
            }
            Info.Records = res;
            Info.ColumnsSelected = res2;
        }

        // This method gathers the datatype of a column from the database
        private string GetDatatype(string column)
        {
            return Tabledata.ColumnsDatatypes[column];
        }

        // This method sets a column to not null
        private void MakeNotNull(string column, string table)
        {
            string name = table + '.' + column;
            Broker.Instance().Run(new SqlCommand("ALTER TABLE " + GetTableSchemaName(table) + " ALTER COLUMN " + column + " " + GetDatatype(name) + " NOT NULL", Con), "type");
        }

        // This method gathers the PK constraint name of a table from the database
        private string GetPKConstraint(string table)
        {
            string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\getPKConstraint.sql");
            string sql = System.IO.File.ReadAllText(path);
            DataSet ds = Broker.Instance().Run(new SqlCommand(sql + table + "'", Con), "schema");
            DataTable dt = ds.Tables["schema"];
            DataRow row = dt.Rows[0];
            return (string)row[0];
        }

        // This method is used to filter the spacial datatypes 
        private string[] FilterSpacial(string table, string[] array)
        {
            int index = array.Length;
            string[] res = new string[index];

            for (int j = 0; j < array.Length; j++)
            {
                string name = table + '.' + array[j];
                string type = GetDatatype(name);
                if (!IsSpacial(type))
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
        public static bool IsSpacial(string type)
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
        public static bool IsDNI(string value)
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
        public static bool IsEmail(string value)
        {
            if (value.Contains('@') && value.Contains('.') && value.IndexOf('@') < value.IndexOf('.'))
            {
                return true;
            }
            return false;
        }

        // This method is used to check if a database cell data corresponds to a Phone Number
        public static bool IsPhone(string value)
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
        public static bool IsCCN(string value)
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
        private void SelectMaskedRecords(string data)
        {
            Dictionary<string, string[]> res = ParseColumnSelection(data);

            foreach (KeyValuePair<string, string[]> record in Info.Records)
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
                                aux_masked[counter] = Info.Records[record.Key + "Masked"][i];
                                counter++;
                            }
                        }

                        Tabledata.Records[record.Key] = aux;
                        Tabledata.Records[record.Key + "Masked"] = aux_masked;
                    }
                }
            }
        }

        // This method saves the selection of the tables selected for the primary_keys functionality
        private void SelectPksTables(string data)
        {
            string[] aux = data.Split(',');
            string[] tables = new string[aux.Length - 1];
            Array.Copy(aux, 1, tables, 0, aux.Length - 1);
            Dictionary<string, string[]> pks = new Dictionary<string, string[]>();
            Dictionary<string, string[]> suggestedPks = new Dictionary<string, string[]>();

            foreach (string table in tables)
            {
                if (ArrayToString(Tabledata.TablePks[table], true) != ArrayToString(Tabledata.TableSuggestedPks[table], true))
                {
                    pks.Add(table, Tabledata.TablePks[table]);
                    suggestedPks.Add(table, Tabledata.TableSuggestedPks[table]);
                }
            }
            Info.TablesSelected = pks.Keys.ToArray();
            Info.TablePks = pks;
            Info.TableSuggestedPks = suggestedPks;
        }

        // This method saves the selection of the tables selected for the foreign_keys functionality
        private void SelectFksTables(string data)
        {
            string[] input = data.Split(',');
            List<Models.Constraint> Fks = new List<Models.Constraint>();
            List<Models.Constraint> SuggestedFks = new List<Models.Constraint>();

            for (int i = 1; i < input.Length; i++)
            {
                Models.Constraint[] aux = Info.TableFks.Where(f => f.Table == input[i].Split('.')[0] && f.Table2 == input[i].Split('.')[1]).ToArray();
                foreach (Models.Constraint c in aux)
                {
                    Fks.Add(c);
                }
                aux = Info.TableSuggestedFks.Where(f => f.Table == input[i].Split('.')[0] && f.Table2 == input[i].Split('.')[1]).ToArray();
                foreach (Models.Constraint c in aux)
                {
                    SuggestedFks.Add(c);
                }
            }

            List<Models.Constraint> other = new List<Models.Constraint>();

            foreach (Models.Constraint c in SuggestedFks)
            {
                if (Fks.Contains(c))
                {
                    other.Add(c);
                }
            }

            foreach (Models.Constraint c in other)
            {
                Fks.Remove(c);
                SuggestedFks.Remove(c);
            }

            Info.TableFks = Fks.ToArray();
            Info.TableSuggestedFks = SuggestedFks.ToArray();
        }

        // This method saves the selection of the records selected for the remove_duplicates functionality
        private void SelectDuplicates(string data)
        {
            data = data.Replace("CheckBox", "");
            Dictionary<string, string[]> parsedData = ParseColumnSelection(data);
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string[]> entry in parsedData)
            {
                res.Add(entry.Key, Info.ColumnsSelected[entry.Key]);

                for (int i = 0; i < entry.Value.Length; i++)
                {
                    entry.Value[i] = entry.Value[i].Replace(entry.Key, "");
                }
                int[] records = entry.Value.Select(s => int.Parse(s.Replace("_", ""))).ToArray();
                for (int k = 0; k < res[entry.Key].Length; k++)
                {
                    string[] aux = new string[records.Length];
                    int index = 0;
                    string name = entry.Key + '.' + res[entry.Key][k];

                    for (int j = 0; j < Info.Records[name].Length; j++)
                    {
                        if (records.Contains(j))
                        {
                            aux[index] = Info.Records[name][j];
                            index++;
                        }
                    }
                    Info.Records[name] = aux;
                }
            }

            Info.ColumnsSelected = res;

        }

        // This method saves the selection of the datatypes selected for the improve_datatypes functionality
        private void SelectDatatypes(string data)
        {
            string[] aux = data.Split(',');
            string[] columns = new string[aux.Length - 1];
            Array.Copy(aux, 1, columns, 0, aux.Length - 1);
            Dictionary<string, string> datatypes = new Dictionary<string, string>();
            Dictionary<string, string> suggestedDatatypes = new Dictionary<string, string>();
            Dictionary<string, string[]> selected = new Dictionary<string, string[]>();
            string table = "";
            int index = 0;

            foreach (string column in columns)
            {
                string[] name = column.Split('.');
                if (table == "" || table != name[0])
                {
                    if (table != "")
                    {
                        string[] other = new string[index];
                        Array.Copy(aux, 0, other, 0, index);
                        selected.Add(table, other);
                    }
                    table = name[0];
                    index = 0;
                    aux = new string[columns.Length];
                }

                if (Tabledata.ColumnsDatatypes[column] != Tabledata.ColumnsSuggestedDatatypes[column])
                {
                    aux[index] = name[1];
                    index++;
                    datatypes.Add(column, Tabledata.ColumnsDatatypes[column]);
                    suggestedDatatypes.Add(column, Tabledata.ColumnsSuggestedDatatypes[column]);
                }
            }

            string[] other2 = new string[index];
            Array.Copy(aux, 0, other2, 0, index);
            selected.Add(table, other2);

            Info.ColumnsSelected = selected;
            Info.ColumnsDatatypes = datatypes;
            Info.ColumnsSuggestedDatatypes = suggestedDatatypes;
        }
        // This method saves the selection of the datatypes selected for the missing_values functionality
        private string SelectMissingValues(string data)
        {
            Dictionary<string, string[]> res = ParseColumnSelection(data);
            Dictionary<string, string[]> newRecords = new Dictionary<string, string[]>();
            Dictionary<string, string[]> newColumns = new Dictionary<string, string[]>();
            string mode = data.Split('/')[0];

            if (mode != "deleteColumns")
            {
                foreach (KeyValuePair<string, string[]> entry in res)
                {
                    foreach (KeyValuePair<string, string[]> record in Tabledata.Records)
                    {
                        if (record.Key.Contains(entry.Key + '.'))
                        {
                            string[] input = entry.Value;
                            if (mode == "updateRows")
                            {
                                input = entry.Value.Where(s => s.Contains('_')).ToArray();
                            }
                            string[] aux = new string[input.Length];
                            string[] indexes = input;
                            string[][] columns = new string[indexes.Length][];
                            string[][] columnsValues = new string[indexes.Length][];
                            if (mode == "updateRows")
                            {
                                indexes = input.Select(s => s.Split('_')[0]).ToArray();
                                for (int x = 0; x < indexes.Length; x++)
                                {
                                    string[] aux2;
                                    aux2 = input[x].Replace(indexes[x] + '_', "").Split('_');
                                    columns[x] = new string[aux2.Length];
                                    columnsValues[x] = new string[aux2.Length];
                                    for (int y = 0; y < aux2.Length; y++)
                                    {
                                        columns[x][y] = aux2[y].Split('=')[0];
                                        columnsValues[x][y] = aux2[y].Split('=')[1];
                                    }
                                }
                            }
                            int counter = 0;
                            for (int i = 0; i < record.Value.Length; i++)
                            {
                                if (indexes.Contains(i.ToString()))
                                {
                                    if (mode == "updateRows" && columns[i].Contains(record.Key.Split('.')[1]))
                                    {
                                        aux[counter] = columnsValues[i][Array.IndexOf(columns[i], record.Key.Split('.')[1])];
                                    }
                                    else
                                    {
                                        aux[counter] = record.Value[i];
                                    }
                                    counter++;
                                }
                            }
                            newRecords.Add(record.Key, aux);
                        }
                    }
                    newColumns.Add(entry.Key, Info.ColumnsSelected[entry.Key]);
                }
                Info.ColumnsSelected = newColumns;
                Info.Records = newRecords;
            }
            else
            {
                string[] splitted = data.Split(',');
                string[] columns = new string[splitted.Length - 1];
                Array.Copy(splitted, 1, columns, 0, splitted.Length - 1);
                res = new Dictionary<string, string[]>();
                string table = "";
                int index = 0;
                foreach (string column in columns)
                {
                    string[] name = column.Split('.');
                    if (table == "" || table != name[0])
                    {
                        if (table != "")
                        {
                            string[] other = new string[index];
                            Array.Copy(splitted, 0, other, 0, index);
                            res.Add(table, other);
                        }
                        table = name[0];
                        index = 0;
                        splitted = new string[columns.Length];
                    }
                    splitted[index] = name[1];
                    index++;
                }
                string[] other2 = new string[index];
                Array.Copy(splitted, 0, other2, 0, index);
                res.Add(table, other2);
                Info.ColumnsSelected = res;
                Info.Records = null;
            }
            return mode;
        }
        // This method saves the selection of the indexes selected for the improve_indexes functionality
        private void SelectIndexes(string data)
        {
            Dictionary<string, string[]> aux = ParseColumnSelection(data);
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            foreach (KeyValuePair<string, string[]> entry in aux)
            {
                res[entry.Key] = new string[entry.Value.Length];
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    res[entry.Key][i] = Tabledata.Indexes[entry.Key][i];
                }
            }
            Info.TablesSelected = res.Keys.ToArray();
            Info.Records = res;
        }

        // This method parses a string into a dictionary
        public Dictionary<string, string[]> ParseColumnSelection(string selection)
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
        public string[] ParseTableSelection(string selection)
        {
            string[] tables = selection.Split('/');
            string[] res = new string[tables.Length - 1];
            Array.Copy(tables, 1, res, 0, res.Length);
            return res;
        }

        //
        public string ArrayToString(object[] array, bool brackets)
        {
            StringBuilder bld = new StringBuilder();
            for (int i = 0; i < array.Length; i++)
            {
                if (i == 0)
                {
                    if (brackets)
                    {
                        bld.Append('[' + array[i].ToString() + ']');
                    }
                    else
                    {
                        bld.Append(array[i].ToString());
                    }

                }
                else
                {
                    if (brackets)
                    {
                        bld.Append('[');
                    }
                    bld.Append(array[i].ToString());
                    if (brackets)
                    {
                        bld.Append(']');
                    }
                }
                if (i != array.Length - 1)
                {
                    bld.Append(',');
                }
            }
            return bld.ToString();
        }

        public static bool StringSimilar(string a, string b)
        {
            if ((a.Length == 0) || (b.Length == 0))
            {
                return false;
            }
            a = a.Replace(",", "");
            b = b.Replace(",", "");
            double maxLen = a.Length > b.Length ? a.Length : b.Length;
            int minLen = a.Length < b.Length ? a.Length : b.Length;
            if (minLen < maxLen - 1)
                return false;
            double LIMIT = ((maxLen - 1) / maxLen);
            int same = 0;
            int index = 0;
            string big, small;
            if (a.Length == maxLen)
            {
                big = a;
                small = b;
            }
            else
            {
                big = b;
                small = a;
            }
            for (int i = 0; i < maxLen; i++)
            {
                if (small[index] == big[i])
                {
                    same++;
                    index++;
                }
                if (index == minLen)
                {
                    i = (int)maxLen;
                }
            }
            if (same >= maxLen * LIMIT)
                return true;
            return false;
        }

    }
}
