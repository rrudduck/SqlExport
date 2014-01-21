using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using NDesk.Options;

namespace SqlExport
{
    class Program
    {
        static void Main( string[] args )
        {
            string tablesFile = null;

            string schemaFile = null;

            string dbFile = null;

            string connectionString = null;

            string output = null;

            var options = new OptionSet
                {
                    { "t=", "File describing which tables to import data for each DB. If this is null, no data is imported.", s => tablesFile = s },
                    { "s=", "File describing which objects to import schema for. If this is null, all schema is imported.", s => schemaFile = s },
                    { "d=", "File describing which databses to import. If this is null, all dbs on the server are imported.", s => dbFile = s },
                    { "c=", "Connection string.", s => connectionString = s },
                    { "o=", "The path to output the sql scripts.", s => output = s }
                };

            options.Parse( args );

            var connection = new ServerConnection( new SqlConnection( connectionString ) );

            var server = new Server( connection );

            foreach ( Database db in server.Databases.Cast<Database>().Where( x => !( LoadFilters( dbFile ).Contains( x.Name ) ) ) )
            {
                if ( db.IsSystemObject ) continue;

                Console.WriteLine( "Processing database {0}.", db.Name );

                var scripter = new Scripter( server );

                var scriptOptions = new ScriptingOptions
                    {
                        AllowSystemObjects = false,
                        AnsiFile = true,
                        AppendToFile = false,
                        Default = true,
                        DriAll = true,
                        Indexes = true,
                        ToFileOnly = true,
                        WithDependencies = true,
                        Triggers = true,
                        Statistics = true
                    };

                scripter.Options = scriptOptions;

                DependencyTree tree = scripter.DiscoverDependencies( GetDatabaseObjects( db ).ToArray(), true );

                DependencyCollection collection = scripter.WalkDependencies( tree );

                File.WriteAllLines( db.Name + ".sql", scripter.ScriptWithList( collection ).OfType<string>() );
            }
        }

        public static IEnumerable<string> LoadFilters( string filename )
        {
            return filename == null ? Enumerable.Empty<string>() : File.ReadAllLines( filename );
        }

        public static IEnumerable<SqlSmoObject> GetDatabaseObjects( Database db )
        {
            var objects = new List<SqlSmoObject>();

            IEnumerable<string> allowedTypes = GetAllowedObjectTypes();

            foreach ( PropertyInfo property in db.GetType().GetProperties() )
            {
                if ( !typeof ( SchemaCollectionBase ).IsAssignableFrom( property.PropertyType ) ) continue;

                var enumerable = property.GetValue( db, null );

                if ( enumerable == null ) continue;

                objects.AddRange( ( (IEnumerable) enumerable ).Cast<SqlSmoObject>().Where( x => allowedTypes.Contains( x.GetType().Name ) ) );
            }

            return objects;
        }

        private static IEnumerable<string> GetAllowedObjectTypes()
        {
            return new[]
                {
                    "UserDefinedFunction",
                    "View",
                    "Table",
                    "StoredProcedure",
                    "Default",
                    "Rule",
                    "Trigger",
                    "UserDefinedAggregate",
                    "Synonym",
                    "Sequence",
                    "UserDefinedDataType",
                    "XmlSchemaCollection",
                    "UserDefinedType",
                    "UserDefinedTableType",
                    "PartitionScheme",
                    "PartitionFunction",
                    "DdlTrigger",
                    "PlanGuide",
                    "SqlAssembly",
                    "UnresolvedEntity"
                };
        }
    }
}
