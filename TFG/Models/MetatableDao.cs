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

                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT [" + mask + pair[1] + "]) FROM " + getTableSchemaName(pair[0]) + " ORDER BY " + getTableColumns(pair[0], true).First().ToString(), con), "records");
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
            return filterSpacial(table, data);
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

        // This method finds the duplicated data from the selected columns and returns it
        public void getDuplicates()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            foreach (KeyValuePair<string, string[]> entry in info.ColumnsSelected)
            {
                string[] columns = getTableColumns(entry.Key, false);
                columns = ArrayToString(columns, true).Split(',');
                columns = columns.Select(x => "a." + x).ToArray();
                string[] SELcolumns = ArrayToString(entry.Value, true).Split(',').Select(x => "b." + x).ToArray();
                string joins = "";
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    int index = Array.IndexOf(getTableColumns(entry.Key, false), entry.Value[i]);
                    if (i == 0)
                    {
                        joins = columns[index] + " = " + SELcolumns[i];
                    }
                    else
                    {
                        joins += " AND " + columns[index] + " = " + SELcolumns[i];
                    }
                }
                DataSet ds = Broker.Instance().Run(
                    new SqlCommand("SELECT " + ArrayToString(columns, false) + " FROM " + getTableSchemaName(entry.Key)
                    + " a JOIN ( SELECT " + ArrayToString(entry.Value, true) + " FROM " + getTableSchemaName(entry.Key)
                    + " GROUP BY " + ArrayToString(entry.Value, true) + " HAVING COUNT(*)>1) b ON " + joins
                    + " ORDER BY " + ArrayToString(entry.Value, true), con), "duplicates");
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
                columns = getTableColumns(entry.Key, false);
                for (int i = 0; i < columns.Length; i++)
                {
                    string name = entry.Key + '.' + columns[i];
                    if (container[i] != null)
                    {
                        res.Add(name, container[i]);
                    }
                }
                info.ColumnsSelected[entry.Key] = columns;
            }
            info.Records = res;
        }

        // This method retrieves the already found missing values from the selected columns
        public void getMissingValue(string missingValue)
        {
            string table = missingValue.Split('.')[0];
            string[] columns = getTableColumns(table, false);
            for (int i = 0; i < columns.Length; i++)
            {
                string name = table + '.' + columns[i];
                info.Records.Add(name, tabledata.MissingValues[name]);
            }
        }

        // This method finds the missing values from the selected columns and saves them
        public void findMissingValues()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            foreach (KeyValuePair<string, string[]> entry in info.ColumnsSelected)
            {
                string[] columns = getTableColumns(entry.Key, false);
                string joins = entry.Value[0] + " IS NULL";
                for (int i = 1; i < entry.Value.Length; i++)
                {
                    joins += " OR " + entry.Value[i] + " IS NULL";
                }
                DataSet ds = Broker.Instance().Run(
                    new SqlCommand("SELECT " + ArrayToString(columns, false) + " FROM " + getTableSchemaName(entry.Key)
                    + " WHERE " + joins, con), "missing");
                DataTable dt = ds.Tables["missing"];
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
                for (int i = 0; i < columns.Length; i++)
                {
                    string name = entry.Key + '.' + columns[i];
                    if (container[i] != null)
                    {
                        res.Add(name, container[i]);
                    }
                    else
                    {
                        res.Add(name, new string[0]);
                    }
                }
                string[] other = new string[columns.Length + entry.Value.Length];
                Array.Copy(columns, 0, other, 0, columns.Length);
                string[] aux = entry.Value.Select(s => s + "Missing").ToArray();
                Array.Copy(aux, 0, other, columns.Length, aux.Length);
                info.ColumnsSelected[entry.Key] = other;
            }
            tabledata.MissingValues = res;
        }

        // This method gets the data of the columns from the database
        private void findRecords()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            foreach (KeyValuePair<string, string[]> entry in getColumns(false))
            {
                string columnsNames = ArrayToString(entry.Value, true);

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
        private void findTableAndColumnData()
        {
            string[] tables = tabledata.Tables;
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            for (int i = 0; i < tables.Length; i++)
            {
                res.Add(tables[i], findTableColumns(tables[i]));
            }

            Dictionary<string, string[]> result = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string[]> entry in res.OrderBy(key => key.Key))
            {
                result.Add(entry.Key, entry.Value);
            }

            tabledata.TablesColumns = result;
        }

        // This method gathers the names of the tables from the database and saves the result to the corresponding Model variable
        private void findTableData()
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

            tabledata.Tables = res;
        }

        // This method gathers the names of the columns of a given table from the database and returns the result
        public Dictionary<string, string[]> getColumns(bool getSpacial)
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            foreach (string table in tabledata.Tables)
            {
                res.Add(table, getTableColumns(table, getSpacial));
            }
            return res;
        }

        // This method gathers the names of the columns of a given table from the database and returns the result
        private string[] getTableColumns(string table, bool getSpacial)
        {
            string[] res = tabledata.TablesColumns[table];

            if (!getSpacial)
            {
                res = filterSpacial(table, res);
            }
            return res;
        }

        // This method gathers the names of the columns of from the database and returns the result
        private string[] findTableColumns(string table)
        {
            DataSet dsC;
            DataTable dtC;

            dsC = Broker.Instance().Run(new SqlCommand("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + table + "'", con), "columns");
            dtC = dsC.Tables["columns"];

            string[] aux = new string[dtC.Rows.Count];

            for (int j = 0; j < dtC.Rows.Count; j++)
            {
                aux[j] = (string)dtC.Rows[j][0];
            }

            return aux;
        }

        private void findConstraints()
        {
            List<Models.Constraint> res = new List<Models.Constraint>();

            string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\findForeign.sql");
            string sql = System.IO.File.ReadAllText(path);
            DataSet ds = Broker.Instance().Run(new SqlCommand(sql, con), "findConstraints");
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
            ds = Broker.Instance().Run(new SqlCommand(sql, con), "findConstraints");
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
            ds = Broker.Instance().Run(new SqlCommand(sql, con), "findConstraints");
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
            ds = Broker.Instance().Run(new SqlCommand(sql, con), "findConstraints");
            dt = ds.Tables["findConstraints"];
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                string name = (string)dt.Rows[i][1];
                string table = (string)dt.Rows[i][0];
                string column = name.Split('_', 3)[2];
                Models.Constraint aux = new Models.Constraint(name, table, column, "INDEX");
                res.Add(aux);
            }

            tabledata.constraints = res.ToArray();
        }

        private void deleteConstraints(string table)
        {
            foreach (Models.Constraint c in tabledata.constraints.Where(s => s.type != "COMPUTED COLUMN" && s.type != "INDEX" && (s.table == table || s.table2 == table)))
            {
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + c.table + " DROP CONSTRAINT " + c.name, con), "findConstraints");
            }
            foreach (Models.Constraint c in tabledata.constraints.Where(s => s.type == "COMPUTED COLUMN" && s.table == table))
            {
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + c.table + " DROP COLUMN " + c.column, con), "findConstraints");
            }
            foreach (Models.Constraint c in tabledata.constraints.Where(s => s.type == "INDEX" && s.table == table))
            {
                Broker.Instance().Run(new SqlCommand("DROP INDEX " + c.name + " ON " + c.table, con), "findConstraints");
            }
        }

        private void replaceConstraints(string table)
        {
            foreach (Models.Constraint c in tabledata.constraints.Where(s => s.type == "COMPUTED COLUMN" && s.table == table))
            {
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + c.table + " ADD " + c.column + " AS " + c.definition, con), "findConstraints");
            }
            foreach (Models.Constraint c in tabledata.constraints.Where(s => s.type == "PRIMARY KEY" && s.table == table))
            {
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + c.table + " ADD CONSTRAINT " + c.name + " PRIMARY KEY(" + c.column + ")", con), "findConstraints");
            }
            foreach (Models.Constraint c in tabledata.constraints.Where(s => s.type == "FOREIGN KEY" && (s.table == table || s.table2 == table)))
            {
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + c.table + " ADD CONSTRAINT " + c.name + " FOREIGN KEY(" + c.column
                    + ") REFERENCES " + c.table2 + " (" + c.column + ") ON DELETE CASCADE ON UPDATE CASCADE", con), "findConstraints");
            }
            foreach (Models.Constraint c in tabledata.constraints.Where(s => s.type == "INDEX" && s.table == table))
            {
                string aux = ArrayToString(c.column.Split('_'), true);
                Broker.Instance().Run(new SqlCommand("CREATE INDEX " + c.name + " ON " + c.table + " (" + aux + ")", con), "findConstraints");
            }
        }


        // This method updates the database with the corresponding changes
        public void update(string data)
        {
            switch (info.Functionality)
            {
                case "create_masks":
                    selectMaskedRecords(data);
                    updateDataMasking();
                    break;
                case "primary_keys":
                    selectPksTables(data);
                    updatePrimaryKeys();
                    break;
                case "remove_duplicates":
                    selectDuplicates(data);
                    deleteDuplicates();
                    break;
                case "improve_datatypes":
                    selectDatatypes(data);
                    updateDatatypes();
                    break;
                case "missing_values":
                    updateMissingValues(selectMissingValues(data));
                    break;
                case "improve_indexes":
                    selectIndexes(data);
                    updateIndexes();
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
                    string pkName = entry.Key + '.' + pks[i];
                    pk_data[i] = tabledata.Records[pkName];
                }

                foreach (string column in entry.Value)
                {
                    string name = entry.Key + '.' + column;
                    string aux = name + "Masked";
                    string[] data_masked = tabledata.Records[aux];
                    string[] data = tabledata.Records[name];

                    for (int i = 0; i < data.Length; i++)
                    {
                        string str = " WHERE";

                        for (int j = 0; j < pks.Length; j++)
                        {
                            string pkName = entry.Key + '.' + pks[j];
                            string type = getDatatype(pkName);
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
                            string type = getDatatype(name);
                            str += " " + column + " = convert(" + type + ", '" + data[i];
                            if (type == "datetime")
                            {
                                str += "', 103)";
                            }
                            else
                            {
                                str += "')";
                            }
                        }

                        Broker.Instance().Run(new SqlCommand("UPDATE " + getTableSchemaName(entry.Key) + " SET " + column + " = '" + data_masked[i] + "'" + str, con), "update");
                    }
                    tabledata.Records[name] = data_masked;
                    tabledata.Records.Remove(aux);
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
                Broker.Instance().Run(new SqlCommand("ALTER TABLE " + getTableSchemaName(entry) + " ADD CONSTRAINT " + constraint_name + " PRIMARY KEY (" + ArrayToString(tabledata.TableSuggestedPks[entry], true) + ")", con), "addPK");
                tabledata.TablePks[entry] = tabledata.TableSuggestedPks[entry];
            }
        }

        // This method updates the database with the corresponding changes for functionality remove_duplicates
        private void deleteDuplicates()
        {
            foreach (KeyValuePair<string, string[]> entry in info.ColumnsSelected)
            {
                string[] pks = tabledata.TablePks[entry.Key];
                if (pks.Length != 0)
                {
                    deleteConstraints(getTableSchemaName(entry.Key));
                    string name1 = entry.Key + '.' + pks[0];
                    for (int j = 0; j < info.Records[name1].Length; j++)
                    {
                        string pkfile = "";

                        for (int i = 0; i < pks.Length; i++)
                        {
                            string name = entry.Key + '.' + pks[i];
                            if (i == 0)
                            {
                                pkfile = pks[i] + " = " + info.Records[name][j];
                            }
                            else
                            {
                                pkfile += " AND " + pks[i] + " = " + info.Records[name][j];
                            }
                        }

                        Broker.Instance().Run(new SqlCommand("DELETE FROM " + getTableSchemaName(entry.Key) + " WHERE " + pkfile, con), "removeDuplicates");
                    }
                    replaceConstraints(getTableSchemaName(entry.Key));
                }
            }
            findRecords();
        }

        // This method updates the database with the corresponding changes for functionality improve_datatypes
        private void updateDatatypes()
        {
            foreach (KeyValuePair<string, string[]> entry in info.ColumnsSelected)
            {
                deleteConstraints(getTableSchemaName(entry.Key));
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name = entry.Key + '.' + entry.Value[i];
                    Broker.Instance().Run(new SqlCommand("ALTER TABLE " + getTableSchemaName(entry.Key) + " ALTER COLUMN " + entry.Value[i] + " " + info.ColumnsSuggestedDatatypes[name] + " NOT NULL", con), "datatypeChange");
                    tabledata.ColumnsDatatypes[name] = tabledata.ColumnsSuggestedDatatypes[name];
                }
                replaceConstraints(getTableSchemaName(entry.Key));
            }
        }

        // This method updates the database with the corresponding changes for functionality missing_values
        private void updateMissingValues(string mode)
        {
            foreach (KeyValuePair<string, string[]> entry in info.ColumnsSelected)
            {
                deleteConstraints(getTableSchemaName(entry.Key));
                string query = "";
                string pkfile = "";
                string[] pks = tabledata.TablePks[entry.Key];
                switch (mode)
                {
                    case "deleteColumns":
                        query = "ALTER TABLE " + getTableSchemaName(entry.Key) + " DROP COLUMN " + entry.Value[0];
                        for (int i = 1; i < entry.Value.Length; i++)
                        {
                            query += "," + entry.Value[i];
                        }
                        Broker.Instance().Run(new SqlCommand(query, con), "missingValues");
                        break;
                    case "updateRows":
                        string updatefile = "";
                        string name = entry.Key + '.' + entry.Value[0];
                        for (int j = 0; j < info.Records[name].Length; j++)
                        {
                            for (int k = 0; k < pks.Length; k++)
                            {
                                string pkname = entry.Key + '.' + pks[k];
                                if (k == 0)
                                {
                                    pkfile = pks[k] + " = " + info.Records[pkname][j];
                                }
                                else
                                {
                                    pkfile += " AND " + pks[k] + " = " + info.Records[pkname][j];
                                }
                            }
                            for (int k = 0; k < entry.Value.Length; k++)
                            {
                                name = entry.Key + '.' + entry.Value[k];
                                if (info.ColumnsSelected[entry.Key].Contains(@entry.Value[k] + "Missing"))
                                {
                                    if (k == 0 || updatefile == "")
                                    {
                                        updatefile = entry.Value[k] + " = ";
                                        if (!int.TryParse(info.Records[name][j], out _))
                                        {
                                            updatefile += "'";
                                        }
                                        updatefile += info.Records[name][j];
                                        if (!int.TryParse(info.Records[name][j], out _))
                                        {
                                            updatefile += "'";
                                        }
                                    }
                                    else
                                    {
                                        updatefile += " AND " + entry.Value[k] + " = ";
                                        if (!int.TryParse(info.Records[name][j], out _))
                                        {
                                            updatefile += "'";
                                        }
                                        updatefile += info.Records[name][j];
                                        if (!int.TryParse(info.Records[name][j], out _))
                                        {
                                            updatefile += "'";
                                        }
                                    }

                                }
                            }
                            Broker.Instance().Run(new SqlCommand("UPDATE " + getTableSchemaName(entry.Key) + " SET " + updatefile + " WHERE " + pkfile, con), "missingValues");
                        }
                        break;
                    case "deleteRows":
                        name = entry.Key + '.' + entry.Value[0];
                        for (int j = 0; j < info.Records[name].Length; j++)
                        {
                            for (int k = 0; k < pks.Length; k++)
                            {
                                string pkname = entry.Key + '.' + pks[k];
                                if (k == 0)
                                {
                                    pkfile = pks[k] + " = " + info.Records[pkname][j];
                                }
                                else
                                {
                                    pkfile += " AND " + pks[k] + " = " + info.Records[pkname][j];
                                }
                            }
                            Broker.Instance().Run(new SqlCommand("DELETE FROM " + getTableSchemaName(entry.Key) + " WHERE " + pkfile, con), "missingValues");
                        }
                        break;
                }
                replaceConstraints(getTableSchemaName(entry.Key));
            }
        }

        // This method updates the database with the corresponding changes for functionality missing_values
        private void updateIndexes()
        {
            foreach (KeyValuePair<string, string[]> entry in info.Records)
            {
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    Broker.Instance().Run(new SqlCommand(entry.Value[i], con), "updateIndexes");
                }
            }
            findIndexes();
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

            foreach (KeyValuePair<string, string[]> entry in getColumns(false))
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
            findTableData();
            findTableAndColumnData();
            findTablesSchemaNames();
            findDatatypes();
            findRecords();
            findSuggestedDatatypes();
            findAvailableMasks();
            findPks();
            findConstraints();
            findIndexes();
        }

        // This method gathers the schema names of the tables from the database
        private void findTablesSchemaNames()
        {
            Dictionary<string, string> res = new Dictionary<string, string>();

            foreach (string table in tabledata.Tables)
            {
                DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT '[' + TABLE_SCHEMA + '].[' + TABLE_NAME + ']' FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME = '" + table + "'", con), "schema");
                DataTable dt = ds.Tables["schema"];
                DataRow row = dt.Rows[0];
                res.Add(table, (string)row[0]);
            }

            tabledata.TablesSchemaNames = res;

        }

        // This method gathers the schema name of a table from the database
        private string getTableSchemaName(string table)
        {
            return tabledata.TablesSchemaNames[table];
        }

        // This method gathers the missing and unused indexes of the tables and columns from the database
        private void findIndexes()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            Dictionary<string, List<string>> aux = new Dictionary<string, List<string>>();

            string path = Path.Combine(Directory.GetCurrentDirectory(), @"Scripts\findUnusedIndexes.sql");
            string sql = System.IO.File.ReadAllText(path);
            DataSet ds = Broker.Instance().Run(new SqlCommand(sql, con), "findUnusedIndexes");
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
            ds = Broker.Instance().Run(new SqlCommand(sql, con), "findMissingIndexes");
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
            tabledata.Indexes = res;
        }

        // This method gathers the datatypes of the columns from the database
        private void findDatatypes()
        {
            Dictionary<string, string> res = new Dictionary<string, string>();

            foreach (KeyValuePair<string, string[]> entry in getColumns(true))
            {
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name = entry.Key + '.' + entry.Value[i];
                    DataSet ds = Broker.Instance().Run(new SqlCommand("SELECT DATA_TYPE, character_maximum_length FROM INFORMATION_SCHEMA.COLUMNS WHERE COLUMN_NAME = '" + entry.Value[i] + "'", con), "type");
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

            tabledata.ColumnsDatatypes = res;
        }

        // This method gathers the datatypes of the columns from the database
        private void findSuggestedDatatypes()
        {
            Dictionary<string, string> res = new Dictionary<string, string>();

            foreach (KeyValuePair<string, string[]> entry in getColumns(true))
            {
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name = entry.Key + '.' + entry.Value[i];
                    string result = "result";

                    if (tabledata.Records.ContainsKey(name) && tabledata.Records[name] != null)
                    {
                        string[] record = tabledata.Records[name];
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

            tabledata.ColumnsSuggestedDatatypes = res;
        }

        // This method retrieves the datatypes information of the selected columns
        public void getIndexes()
        {
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            foreach (string entry in info.TablesSelected)
            {
                if (tabledata.Indexes.ContainsKey(entry))
                {
                    res.Add(entry, tabledata.Indexes[entry]);
                }
            }
            info.Records = res;
        }

        // This method retrieves the datatypes information of the selected columns
        public void getDatatypes()
        {
            Dictionary<string, string> res = new Dictionary<string, string>();
            Dictionary<string, string> res_sug = new Dictionary<string, string>();

            foreach (KeyValuePair<string, string[]> entry in info.ColumnsSelected)
            {
                Models.Constraint[] c = tabledata.constraints.Where(s => s.type == "COMPUTED COLUMN" && s.table == getTableSchemaName(entry.Key)).ToArray();
                if (c.Length > 0)
                {
                    string[] aux = c.Select(c => c.column).ToArray();
                    info.ColumnsSelected[entry.Key] = entry.Value.Where(c => !aux.Contains(c)).ToArray();
                }
            }
            foreach (KeyValuePair<string, string[]> entry in info.ColumnsSelected)
            {
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    string name = entry.Key + '.' + entry.Value[i];
                    res.Add(name, tabledata.ColumnsDatatypes[name]);
                    res_sug.Add(name, tabledata.ColumnsSuggestedDatatypes[name]);
                }
            }
            info.ColumnsDatatypes = res;
            info.ColumnsSuggestedDatatypes = res_sug;
        }

        // This method gathers the datatype of a column from the database
        private string getDatatype(string column)
        {
            return tabledata.ColumnsDatatypes[column];
        }

        // This method sets a column to not null
        private void makeNotNull(string column, string table)
        {
            string name = table + '.' + column;
            Broker.Instance().Run(new SqlCommand("ALTER TABLE " + getTableSchemaName(table) + " ALTER COLUMN " + column + " " + getDatatype(name) + " NOT NULL", con), "type");
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
        private string[] filterSpacial(string table, string[] array)
        {
            int index = array.Length;
            string[] res = new string[index];

            for (int j = 0; j < array.Length; j++)
            {
                string name = table + '.' + array[j];
                string type = getDatatype(name);
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
                if (ArrayToString(tabledata.TablePks[table], true) != ArrayToString(tabledata.TableSuggestedPks[table], true))
                {
                    pks.Add(table, tabledata.TablePks[table]);
                    suggestedPks.Add(table, tabledata.TableSuggestedPks[table]);
                }
            }
            info.TablesSelected = pks.Keys.ToArray();
            info.TablePks = pks;
            info.TableSuggestedPks = suggestedPks;
        }

        // This method saves the selection of the records selected for the remove_duplicates functionality
        private void selectDuplicates(string data)
        {
            data = data.Replace("CheckBox", "");
            Dictionary<string, string[]> parsedData = parseColumnSelection(data);
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();

            foreach (KeyValuePair<string, string[]> entry in parsedData)
            {
                res.Add(entry.Key, info.ColumnsSelected[entry.Key]);

                for (int i = 0; i < entry.Value.Length; i++)
                {
                    entry.Value[i] = entry.Value[i].Replace(entry.Key, "");
                }
                int[] records = entry.Value.Select(int.Parse).ToArray();
                for (int k = 0; k < res[entry.Key].Length; k++)
                {
                    string[] aux = new string[records.Length];
                    int index = 0;
                    string name = entry.Key + '.' + res[entry.Key][k];

                    for (int j = 0; j < info.Records[name].Length; j++)
                    {
                        if (records.Contains(j))
                        {
                            aux[index] = info.Records[name][j];
                            index++;
                        }
                    }
                    info.Records[name] = aux;
                }
            }

            info.ColumnsSelected = res;

        }

        // This method saves the selection of the datatypes selected for the improve_datatypes functionality
        private void selectDatatypes(string data)
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

                if (tabledata.ColumnsDatatypes[column] != tabledata.ColumnsSuggestedDatatypes[column])
                {
                    aux[index] = name[1];
                    index++;
                    datatypes.Add(column, tabledata.ColumnsDatatypes[column]);
                    suggestedDatatypes.Add(column, tabledata.ColumnsSuggestedDatatypes[column]);
                }
            }

            string[] other2 = new string[index];
            Array.Copy(aux, 0, other2, 0, index);
            selected.Add(table, other2);

            info.ColumnsSelected = selected;
            info.ColumnsDatatypes = datatypes;
            info.ColumnsSuggestedDatatypes = suggestedDatatypes;
        }
        // This method saves the selection of the datatypes selected for the missing_values functionality
        private string selectMissingValues(string data)
        {
            Dictionary<string, string[]> res = parseColumnSelection(data);
            Dictionary<string, string[]> newRecords = new Dictionary<string, string[]>();
            Dictionary<string, string[]> newColumns = new Dictionary<string, string[]>();
            string mode = data.Split('/')[0];

            if (mode != "deleteColumns")
            {
                foreach (KeyValuePair<string, string[]> entry in res)
                {
                    foreach (KeyValuePair<string, string[]> record in tabledata.Records)
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
                    newColumns.Add(entry.Key, info.ColumnsSelected[entry.Key]);
                }
                info.ColumnsSelected = newColumns;
                info.Records = newRecords;
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
                info.ColumnsSelected = res;
                info.Records = null;
            }
            return mode;
        }
        // This method saves the selection of the indexes selected for the improve_indexes functionality
        private void selectIndexes(string data)
        {
            Dictionary<string, string[]> aux = parseColumnSelection(data);
            Dictionary<string, string[]> res = new Dictionary<string, string[]>();
            foreach (KeyValuePair<string, string[]> entry in aux)
            {
                res[entry.Key] = new string[entry.Value.Length];
                for (int i = 0; i < entry.Value.Length; i++)
                {
                    res[entry.Key][i] = tabledata.Indexes[entry.Key][i];
                }
            }
            info.TablesSelected = res.Keys.ToArray();
            info.Records = res;
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
        private string ArrayToString(object[] array, bool brackets)
        {
            string res = "";
            for (int i = 0; i < array.Length; i++)
            {
                if (i == 0)
                {
                    if (brackets)
                    {
                        res = '[' + array[i].ToString() + ']';
                    }
                    else
                    {
                        res = array[i].ToString();
                    }

                }
                else
                {
                    if (brackets)
                    {
                        res += '[';
                    }
                    res += array[i].ToString();
                    if (brackets)
                    {
                        res += ']';
                    }
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
