using Shared.dto.threading;
using System.Collections.Generic;
using Shared.dto.SqlServer;
using System.Data.SqlClient;
using Shared;

namespace SqlServerDb
{
    public class SqlServerDbMain : Shared.dto.Main
    {
        public SqlServerDbMain(string pDbconn,
                               int pMaxThreads,
                               long sourceRecordTotal)
        {
            this.Credentials = new SqlServerStorageCredentials(pDbconn);
            this.StorageType = Enumeration.StorageTypes.SqlServer;
            this.DataJobs = new List<ThreadJob>();
            this.ThreadsComplete = new List<ThreadCompletion>();
            this.testCompletion = new BlobTestCompletion();
            this.MaxThreadsAllowed = pMaxThreads;

            if (sourceRecordTotal > 0)
                this.TotalRecordCount = sourceRecordTotal;
            else
                SetRecordCountTotal();
        }

        public void Run()
        {
            Setup();
            RunExample();
        }

        #region Setup

        private void Setup()
        {
            string sql = " IF OBJECT_ID (N'Updates', N'U') IS NOT NULL  "
                        + "    Drop table dbo.Updates; "
                        + "  "
                        + " CREATE TABLE [dbo].[Updates](  "
                        + "         [id][bigint] IDENTITY(1, 1) NOT NULL,  "
                        + "         [type] [varchar](250) NULL,  "
                        + "         [data] [varchar](max) NULL,  "
                        + "         [created] [datetime]  "
                        + "     NULL CONSTRAINT[DF_Updates_created]  DEFAULT(getdate()),  "
                        + " CONSTRAINT[PK_Updates] PRIMARY KEY CLUSTERED  "
                        + " (  "
                        + "    [id] ASC  "
                        + " )WITH(PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON[PRIMARY]  "
                        + " ) ON[PRIMARY] TEXTIMAGE_ON[PRIMARY]";

            SqlCommand cmd = Utilities.GetCommand(this.Credentials);
            cmd.CommandType = System.Data.CommandType.Text;
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
            Utilities.CloseCmd(cmd);
        }

        #endregion
    }
}
