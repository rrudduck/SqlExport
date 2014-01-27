using System.Collections.Specialized;
using System.Text;

namespace SqlExport
{
    public class Script
    {
        private readonly StringBuilder _builder = new StringBuilder();

        public void Add( string sql )
        {
            _builder.AppendLine( sql );
        }

        public void Add( StringCollection strings )
        {
            foreach ( string str in strings )
            {
                _builder.AppendLine( str );
            }

            _builder.AppendLine( "GO" );

            _builder.AppendLine();
        }

        public override string ToString()
        {
            return _builder.ToString();
        }
    }
}
