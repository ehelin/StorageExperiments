using System;
using Shared.dto.threading;
using System.ComponentModel;
using System.Data.SqlClient;
using Shared.dto.source;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Shared.dto.SqlServer
{
    public class SqlServerLoadThreadJob : ThreadJob
    {
        public SqlServerLoadThreadJob() : base() { }

        public SqlServerLoadThreadJob(DataStorageCredentials pCredentials,
                                     long recordCount,
                                     long startId,
                                     int threadId) : base(pCredentials, recordCount, startId, threadId)
        { }

        protected async override void RunLoad(int pThreadId, long pRecordCount, DataStorageCredentials pCredentials, long pStartId, long pEndPoint)
        {
            LoadRecords(pThreadId, pRecordCount, pCredentials, pStartId, pEndPoint);
        }

        private async void LoadRecords(int pThreadId, long pRecordCount, DataStorageCredentials pCredentials, long pStartId, long pEndPoint)
        {
            Database db = new Database();
            long threadId = pThreadId;
            long recordCount = pRecordCount;
            long startId = pStartId;
            long endId = pEndPoint;

            Console.WriteLine("Thread " + threadId + " (RcrdCnt/Start/End) (" + recordCount.ToString() + "/" 
                                + startId.ToString() + "/" + endId.ToString() + ")" + DateTime.Now.ToString());

            while (startId <= endId)
            {
                try
                {
                    IList<SourceRecord> scrRecords = db.GetRecord(startId, endId);

                    foreach (SourceRecord sr in scrRecords)
                        InsertRecord(sr, pCredentials, threadId);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }

                startId = startId + Convert.ToInt64(SourceDataConstants.SQL_GET_TOP_VALUE);
            }

            Console.WriteLine("Thread " + threadId + " Done! " + DateTime.Now.ToString());
        }
        private void InsertRecord(SourceRecord sr, DataStorageCredentials pCredentials, long pThreadName)
        {
            SqlCommand cmd = null;
            bool inserted = false;
            int errorTryCtr = 0;

            while (!inserted)
            {
                try
                {
                    SqlServerStorageCredentials credentials = (SqlServerStorageCredentials)pCredentials;
                    cmd = Utilities.GetCommand(credentials);
                    cmd.CommandType = System.Data.CommandType.Text;
                    //cmd.CommandText = "if not exists(select Id  "
                    //                   + " from dbo.UpdatesCloudTable  "
                    //                    + "  where Id = @originalId  "
                    //                    + "  or[type] = @type  "
                    //                    + "  or data = @data  "
                    //                    + "  or created = @created)  "
                    //                    + "  begin  "
                    //                        + "  INSERT INTO[dbo].[UpdatesCloudTable] select @originalId, @type, @data, @created  "
                    //                    + "  end ";
                    cmd.CommandText = " INSERT INTO[dbo].[UpdatesCloudTable] select @originalId, @type, @data, @created ";
                    cmd.Parameters.Clear();

                    cmd.Parameters.Add(new SqlParameter("@originalId", sr.Id));
                    cmd.Parameters.Add(new SqlParameter("@type", pThreadName.ToString() + "-" + sr.Type));
                    cmd.Parameters.Add(new SqlParameter("@data", sr.Data));
                    cmd.Parameters.Add(new SqlParameter("@created", sr.Created));

                    cmd.ExecuteNonQuery();

                    inserted = true;
                }
                catch (Exception e)
                {
                    if (errorTryCtr > Constants.MAX_RETRY)
                    {
                        Console.WriteLine("SqlServerLoad - Max error limit reached...moving on");
                        break;
                    }
                    else
                    {
                        Console.WriteLine("SqlServerLoad ERROR: - " + e.Message);
                        errorTryCtr++;
                    }
                }
                finally
                {
                    Utilities.CloseCmd(cmd);
                }
            }
        }

        public override void RunCountQueries(DataStorageCredentials pCredentials)
        {
            GetRecordCount(pCredentials);
            GetSpecificId(pCredentials);
            GetCountForSpecificType(pCredentials);
        }
        private void GetRecordCount(DataStorageCredentials pCredentials)
        {
            string sql = "select count(*) from [dbo].[UpdatesCloudTable]";

            this.RunSqlQuery(sql, pCredentials, "Sql Server Record Count");
        }
        private void GetSpecificId(DataStorageCredentials pCredentials)
        {
            string sql = "select count(*) from [dbo].[UpdatesCloudTable] where Id = " + this.TestRecordId.ToString();

            this.RunSqlQuery(sql, pCredentials, "Sql Server Record Specific Id");
        }
        private void GetCountForSpecificType(DataStorageCredentials pCredentials)
        {
            string sql = "select count(*) from [dbo].[UpdatesCloudTable] where [type] = " + this.TestType;

            this.RunSqlQuery(sql, pCredentials, "Sql Server Record Count for Specific Type");
        }
    }
}

