using System;
using Shared.dto.threading;
using System.ComponentModel;
using System.Data.SqlClient;
using Shared.dto.source;

namespace Shared.dto.SqlServer
{
    public class SqlServerLoadThreadJob : ThreadJob
    {
        public SqlServerLoadThreadJob() : base() { }

        public SqlServerLoadThreadJob(ThreadCompletion pDone,
                                     DataStorageCredentials pCredentials,
                                     long recordCount,
                                     long startId,
                                     int threadId) : base(pDone, pCredentials, recordCount, startId, threadId)
        { }

        public override void DoWork(object sender, DoWorkEventArgs e)
        {
            Database db = new Database();
            SqlCommand cmd = null;
            int ctr = 0;

            Console.WriteLine("Thread " + threadId + " starting with " + this.recordCount.ToString() + " records! " + DateTime.Now.ToString());

            while (ctr <= this.recordCount)
            {
                try
                {
                    cmd = Utilities.GetCommand(this.Credentials);
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.CommandText = "INSERT INTO [dbo].[Updates] select @type, @data, @created";

                    SourceRecord sr = db.GetRecord(startId);

                    InsertRecord(sr, cmd);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: " + ex.Message);
                }
                finally
                {
                    Utilities.CloseCmd(cmd);
                }

                startId++;
                ctr++;
            }

            Console.WriteLine("Thread " + threadId + " Done! " + DateTime.Now.ToString());
        }

        private void InsertRecord(SourceRecord sr, SqlCommand cmd)
        {
            cmd.Parameters.Clear();

            cmd.Parameters.Add(new SqlParameter("@type", sr.Type));
            cmd.Parameters.Add(new SqlParameter("@data", sr.Data));
            cmd.Parameters.Add(new SqlParameter("@created", sr.Created));

            cmd.ExecuteNonQuery();
        }
    }
}

