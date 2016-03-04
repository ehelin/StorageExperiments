using System;
using Shared.dto.threading;
using System.Data.SqlClient;

namespace Shared.dto.SqlServer
{
    public class BlobTestCompletion : TestComplete
    {
        public override void HandleTestComplete(DataStorageCredentials Credentials)
        {
            //TestComplete.RunUpdateCount();

            SqlServerStorageCredentials ssc = (SqlServerStorageCredentials)Credentials;

            SqlCommand cmd = Utilities.GetCommand(ssc);
            cmd.CommandType = System.Data.CommandType.Text;
            cmd.CommandText = "select count(*) from [dbo].[Updates]";
            cmd.CommandTimeout = 600000000;         //required because of the size of the tables
            SqlDataReader dr = cmd.ExecuteReader();
            string result = string.Empty;

            if (dr.Read())
                result = Utilities.GetSafeString(dr[0]);

            Utilities.CloseRdr(dr);

            Console.WriteLine("There are " + result + " records!");
        }
    }
}
