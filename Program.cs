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
                DriDefaults = true,
                DriUniqueKeys = true,
                SchemaQualify = true,
                IncludeIfNotExists = true,
                NoFileGroup = true,
            };

        static void Main( string[] args )
        {
            string tablesFile = null;

            string schemaFile = null;

            string dbFile = null;

            string connectionString = null;

            string output = null;

            string dbName = null;

            var options = new OptionSet
                {
                    { "t=", "File describing which tables to import data for each DB. If this is null, no data is imported.", s => tablesFile = s },
                    { "s=", "File describing which objects to import schema for. If this is null, all schema is imported.", s => schemaFile = s },
                    { "d=", "File describing which databses to import. If this is null, all dbs on the server are imported.", s => dbFile = s },
                    { "db=", "The name of the database to export if running for a single database.", s => dbName = s },
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
                    if ( !String.IsNullOrEmpty( dbName ) && dbName != db.Name ) continue;

                    if ( db.IsSystemObject ) continue;

                    Console.WriteLine( "Processing database {0}.", db.Name );

                    var script = new Script();

                    script.Add(
                        db.Script(
                            new ScriptingOptions
                                {
                                    NoFileGroup = true,
                                    IncludeIfNotExists = true
                                } ) );

                    script.Add( String.Format( "Use {0};", db.Name ) );

                    foreach ( var collection in GetDbCollections( db ) )
                    {
                        var tables = collection as IEnumerable<Table>;

                        if ( tables != null )
                        {
                            ScriptTables( tables, script );
                        }
                        else
                        {
                            foreach ( dynamic obj in ApplySystemFilter( collection ) )
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
                    Console.WriteLine( "Could not export db {0}. Error was {1}.", db.Name, e.Message );
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

        private static void ScriptTables( IEnumerable<Table> tables, Script script )
        {
            foreach ( Table table in ApplySystemFilter( tables ) )
            {
                script.Add( table.Script( _defaultOptions ) );

                foreach ( Index index in table.Indexes )
                {
                    script.Add( index.Script( _defaultOptions ) );
                }

                foreach ( Trigger trigger in table.Triggers )
                {
                    script.Add( trigger.Script( _defaultOptions ) );
                }

                foreach ( Statistic statistic in table.Statistics )
                {
                    script.Add( statistic.Script( _defaultOptions ) );
                }
            }

            foreach ( Table table in tables )
            {
                foreach ( ForeignKey key in table.ForeignKeys )
                {
                    script.Add( key.Script( _defaultOptions ) );
                }

                foreach ( Check check in table.Checks )
                {
                    script.Add( check.Script( _defaultOptions ) );
                }
            }
        }

        private static IEnumerable<dynamic> ApplySystemFilter( IEnumerable<dynamic> objects )
        {
            var results = new List<dynamic>();

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

                results.Add( obj );
            }

            return results;
        }
    }
}
