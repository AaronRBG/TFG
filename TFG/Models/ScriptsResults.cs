using System;

namespace TFG.Models
{
    public class ScriptsResults
    {
        public ScriptsResults(string database, string functionality, string[][] results)
        {
            this.database = database;
            this.functionality = functionality;
            this.results = results;
        }

        public ScriptsResults(string database)
        {
            this.database = database;
        }

        public ScriptsResults(string database, string functionality)
        {
            this.database = database;
            this.functionality = functionality;
        }

        public string database { get; set; }
        public string functionality { get; set; }
        public string[][] results { get; set; }

    }
}
