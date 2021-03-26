using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace TFG
{
    public class Broker
    {
        private readonly SqlDataAdapter adp = new SqlDataAdapter();

        private static Broker _instance;

        public Broker() { }

        public static Broker Instance()

        {
            if (_instance == null)
            {
                _instance = new Broker();
            }
            return _instance;
        }

        public DataSet Run(SqlCommand comm, string reff) {
            
            DataSet ds = new DataSet();
            try
            {
                adp.SelectCommand = comm;
                adp.Fill(ds, reff);
            } catch (Exception e)
            {
                Console.WriteLine();
            }

            return ds;
        }
    }
}
