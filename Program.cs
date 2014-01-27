using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using NDesk.Options;

namespace SqlExport
{
    class Program
    {
        private static readonly ScriptingOptions _defaultOptions = new ScriptingOptions
            {
                Default = true,
                SchemaQualify = true,
                IncludeIfNotExists = true,
                NoFileGroup = true,
                DriAll = false,
            };

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

            IEnumerable<string> filters = LoadFilters( dbFile );

            foreach ( Database db in server.Databases.Cast<Database>().Where( x => filters.Contains( x.Name ) || !filters.Any() ) )
            {
                try
                {
                    if ( db.IsSystemObject ) continue;

                    Console.WriteLine( "Processing database {0}.", db.Name );

                    var script = new Script();

                    script.Add(
                        db.Script(
                            new ScriptingOptions
                                {
                                    NoFileGroup = true,
                                    Default = true,
                                    IncludeIfNotExists = true
                                } ) );

                    script.Add( String.Format( "GO; Use {0};", db.Name ) );

                    foreach ( var collection in GetDbCollections( db ) )
                    {
                        if ( collection is IEnumerable<Table> || collection is IEnumerable<View> )
                        {
                            ScriptComplex( collection, script );
                        }
                        else
                        {
                            foreach ( dynamic obj in collection )
                            {
                                Console.WriteLine( "Processing object: {0}", obj.Urn );

                                try
                                {
                                    script.Add( obj.Script( _defaultOptions ) );
                                }
                                catch ( Exception e )
                                {
                                    Console.WriteLine( "Error processing object {0} => {1}", obj.Urn, e.Message );
                                }
                            }
                        }
                    }

                    File.WriteAllText( db.Name + ".sql", script.ToString() );
                }
                catch ( Exception e )
                {
                    Console.WriteLine( "Could not export db {0}.", db.Name );
                }
            }
        }

        public static IEnumerable<string> LoadFilters( string filename )
        {
            return filename == null ? Enumerable.Empty<string>() : File.ReadAllLines( filename );
        }

        private static IEnumerable<IEnumerable<SqlSmoObject>> GetDbCollections( Database db )
        {
            return new IEnumerable<SqlSmoObject>[]
                {
                    db.Schemas.Cast<Schema>().ToList(),
                    db.UserDefinedTypes.Cast<UserDefinedType>().ToList(),
                    db.UserDefinedTableTypes.Cast<UserDefinedTableType>().ToList(),
                    db.UserDefinedDataTypes.Cast<UserDefinedDataType>().ToList(),
                    db.FullTextCatalogs.Cast<FullTextCatalog>().ToList(),
                    db.FullTextStopLists.Cast<FullTextStopList>().ToList(),
                    db.Tables.Cast<Table>().ToList(),
                    db.Views.Cast<View>().ToList(),
                    db.StoredProcedures.Cast<StoredProcedure>().ToList(),
                    db.UserDefinedFunctions.Cast<UserDefinedFunction>().ToList()
                };
        }
 
        private static void ScriptComplex( IEnumerable<dynamic> objects, Script script )
        {
            foreach ( dynamic obj in ApplySystemFilter( objects ) )
            {
                Console.WriteLine( "Processing object: {0}", obj.Urn );

                try
                {
                    script.Add( obj.Script( _defaultOptions ) );

                    script.Add(
                        obj.Script(
                            new ScriptingOptions
                                {
                                    Indexes = true,
                                    SchemaQualify = true,
                                    IncludeIfNotExists = true
                                } ) );

                    foreach ( dynamic trigger in obj.Triggers )
                    {
                        script.Add( trigger.Script( _defaultOptions ) );
                    }

                    foreach ( dynamic statistic in obj.Statistics )
                    {
                        script.Add( statistic.Script( _defaultOptions ) );
                    }
                }
                catch ( Exception e )
                {
                    Console.WriteLine( "Error processing object {0} => {1}", obj.Urn, e.Message );
                }
            }

            foreach ( dynamic obj in ApplySystemFilter( objects ) )
            {
                Console.WriteLine( "Processing second pass: {0}", obj.Urn );

                try
                {
                    script.Add(
                        obj.Script(
                            new ScriptingOptions
                                {
                                    DriAll = true,
                                    SchemaQualify = true,
                                    IncludeIfNotExists = true
                                } ) );
                }
                catch ( Exception e )
                {
                    Console.WriteLine( "Error processing object {0} => {1}", obj.Urn, e.Message );
                }
            }
        }

        private static IEnumerable<dynamic> ApplySystemFilter( IEnumerable<dynamic> objects )
        {
            foreach ( dynamic obj in objects )
            {
                try
                {
                    if ( obj.IsSystemObject ) continue;
                }
                catch ( Exception e )
                {
                    Console.WriteLine( "Error processing object {0} => {1}", obj.Urn, e.Message );
                }

                yield return obj;
            }
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
