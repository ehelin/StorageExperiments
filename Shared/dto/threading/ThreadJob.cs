using System.Collections.Generic;
using System.Threading.Tasks;
using Shared.dto;
using Shared.dto.SqlServer;
using System.Data.SqlClient;
using System;

namespace Shared.dto.threading
{
    public class ThreadJob
    {
        protected DataStorageCredentials Credentials { get; set; }
        protected long recordCount = 0;
        protected long startId = 0;
        protected int threadId = 0;

        //TODO - make constants
        protected long TestRecordId = 233101;
        protected string TestType = "SouthEast"; 

        public ThreadJob() { }

        public ThreadJob(DataStorageCredentials pCredentials,
                        long pRecordCount,
                        long pStartId,
                        int pThreadId)
        {
            this.Credentials = pCredentials;
            this.recordCount = pRecordCount;
            this.startId = pStartId;
            this.threadId = pThreadId;
        }

        //HACK alert!! I tried spinning up the threads in a loop, but I got wierd results.  After reviewing a 
        //post (see below), I opted to go this route until I could find something less hackish.
        //http://stackoverflow.com/questions/30225476/task-run-with-parameters
        public virtual async void LaunchThreads()
        {
            List<Task> tasks = new List<Task>();

            long thread1StartId = 1;
            long thread1EndId = thread1StartId + this.recordCount;

            long thread2StartId = thread1EndId + 1;
            long thread2EndId = thread2StartId + this.recordCount;

            long thread3StartId = thread2EndId + 1;
            long thread3EndId = thread3StartId + this.recordCount;

            long thread4StartId = thread3EndId + 1;
            long thread4EndId = thread4StartId + this.recordCount;

            long thread5StartId = thread4EndId + 1;
            long thread5EndId = thread5StartId + this.recordCount;

            long thread6StartId = thread5EndId + 1;
            long thread6EndId = thread6StartId + this.recordCount;

            long thread7StartId = thread6EndId + 1;
            long thread7EndId = thread7StartId + this.recordCount;

            long thread8StartId = thread7EndId + 1;
            long thread8EndId = thread8StartId + this.recordCount;

            long thread9StartId = thread8EndId + 1;
            long thread9EndId = thread9StartId + this.recordCount;

            long thread10StartId = thread9EndId + 1;
            long thread10EndId = thread10StartId + this.recordCount;

            long thread11StartId = thread10EndId + 1;
            long thread11EndId = thread11StartId + this.recordCount;

            long thread12StartId = thread11EndId + 1;
            long thread12EndId = thread12StartId + this.recordCount;

            long thread13StartId = thread12EndId + 1;
            long thread13EndId = thread13StartId + this.recordCount;

            long thread14StartId = thread13EndId + 1;
            long thread14EndId = thread14StartId + this.recordCount;

            long thread15StartId = thread14EndId + 1;
            long thread15EndId = thread15StartId + this.recordCount;

            long thread16StartId = thread15EndId + 1;
            long thread16EndId = thread16StartId + this.recordCount;

            long thread17StartId = thread16EndId + 1;
            long thread17EndId = thread17StartId + this.recordCount;

            long thread18StartId = thread17EndId + 1;
            long thread18EndId = thread18StartId + this.recordCount;

            long thread19StartId = thread18EndId + 1;
            long thread19EndId = thread19StartId + this.recordCount;

            long thread20StartId = thread19EndId + 1;
            long thread20EndId = thread20StartId + this.recordCount;

            long thread21StartId = thread20EndId + 1;
            long thread21EndId = thread21StartId + this.recordCount;

            long thread22StartId = thread21EndId + 1;
            long thread22EndId = thread22StartId + this.recordCount;

            long thread23StartId = thread22EndId + 1;
            long thread23EndId = thread23StartId + this.recordCount;

            long thread24StartId = thread23EndId + 1;
            long thread24EndId = thread24StartId + this.recordCount;

            long thread25StartId = thread24EndId + 1;
            long thread25EndId = thread25StartId + this.recordCount;

            long thread26StartId = thread25EndId + 1;
            long thread26EndId = thread26StartId + this.recordCount;

            long thread27StartId = thread26EndId + 1;
            long thread27EndId = thread27StartId + this.recordCount;

            long thread28StartId = thread27EndId + 1;
            long thread28EndId = thread28StartId + this.recordCount;

            long thread29StartId = thread28EndId + 1;
            long thread29EndId = thread29StartId + this.recordCount;

            long thread30StartId = thread29EndId + 1;
            long thread30EndId = thread30StartId + this.recordCount;

            long thread31StartId = thread30EndId + 1;
            long thread31EndId = thread31StartId + this.recordCount;

            long thread32StartId = thread31EndId + 1;
            long thread32EndId = thread32StartId + this.recordCount;

            tasks.Add(Task.Run(() => RunLoad(1, this.recordCount, this.Credentials, thread1StartId, thread1EndId)));
            tasks.Add(Task.Run(() => RunLoad(2, this.recordCount, this.Credentials, thread2StartId, thread2EndId)));
            tasks.Add(Task.Run(() => RunLoad(3, this.recordCount, this.Credentials, thread3StartId, thread3EndId)));
            tasks.Add(Task.Run(() => RunLoad(4, this.recordCount, this.Credentials, thread4StartId, thread4EndId)));
            tasks.Add(Task.Run(() => RunLoad(5, this.recordCount, this.Credentials, thread5StartId, thread5EndId)));
            tasks.Add(Task.Run(() => RunLoad(6, this.recordCount, this.Credentials, thread6StartId, thread6EndId)));
            tasks.Add(Task.Run(() => RunLoad(7, this.recordCount, this.Credentials, thread7StartId, thread7EndId)));
            tasks.Add(Task.Run(() => RunLoad(8, this.recordCount, this.Credentials, thread8StartId, thread8EndId)));
            tasks.Add(Task.Run(() => RunLoad(9, this.recordCount, this.Credentials, thread9StartId, thread9EndId)));
            tasks.Add(Task.Run(() => RunLoad(10, this.recordCount, this.Credentials, thread10StartId, thread10EndId)));
            tasks.Add(Task.Run(() => RunLoad(11, this.recordCount, this.Credentials, thread11StartId, thread11EndId)));
            tasks.Add(Task.Run(() => RunLoad(12, this.recordCount, this.Credentials, thread12StartId, thread12EndId)));
            tasks.Add(Task.Run(() => RunLoad(13, this.recordCount, this.Credentials, thread13StartId, thread13EndId)));
            tasks.Add(Task.Run(() => RunLoad(14, this.recordCount, this.Credentials, thread14StartId, thread14EndId)));
            tasks.Add(Task.Run(() => RunLoad(15, this.recordCount, this.Credentials, thread15StartId, thread15EndId)));
            tasks.Add(Task.Run(() => RunLoad(16, this.recordCount, this.Credentials, thread16StartId, thread16EndId)));
            tasks.Add(Task.Run(() => RunLoad(17, this.recordCount, this.Credentials, thread17StartId, thread17EndId)));
            tasks.Add(Task.Run(() => RunLoad(18, this.recordCount, this.Credentials, thread18StartId, thread18EndId)));
            tasks.Add(Task.Run(() => RunLoad(19, this.recordCount, this.Credentials, thread19StartId, thread19EndId)));
            tasks.Add(Task.Run(() => RunLoad(20, this.recordCount, this.Credentials, thread20StartId, thread20EndId)));
            tasks.Add(Task.Run(() => RunLoad(21, this.recordCount, this.Credentials, thread21StartId, thread21EndId)));
            tasks.Add(Task.Run(() => RunLoad(22, this.recordCount, this.Credentials, thread22StartId, thread22EndId)));
            tasks.Add(Task.Run(() => RunLoad(23, this.recordCount, this.Credentials, thread23StartId, thread23EndId)));
            tasks.Add(Task.Run(() => RunLoad(24, this.recordCount, this.Credentials, thread24StartId, thread24EndId)));
            tasks.Add(Task.Run(() => RunLoad(25, this.recordCount, this.Credentials, thread25StartId, thread25EndId)));
            tasks.Add(Task.Run(() => RunLoad(26, this.recordCount, this.Credentials, thread26StartId, thread26EndId)));
            tasks.Add(Task.Run(() => RunLoad(27, this.recordCount, this.Credentials, thread27StartId, thread27EndId)));
            tasks.Add(Task.Run(() => RunLoad(28, this.recordCount, this.Credentials, thread28StartId, thread28EndId)));
            tasks.Add(Task.Run(() => RunLoad(29, this.recordCount, this.Credentials, thread29StartId, thread29EndId)));
            tasks.Add(Task.Run(() => RunLoad(30, this.recordCount, this.Credentials, thread30StartId, thread30EndId)));
            tasks.Add(Task.Run(() => RunLoad(31, this.recordCount, this.Credentials, thread31StartId, thread31EndId)));
            tasks.Add(Task.Run(() => RunLoad(32, this.recordCount, this.Credentials, thread32StartId, thread32EndId)));

            await Task.WhenAll(tasks);

            RunCountQueries(this.Credentials);
        }

        protected async virtual void RunLoad(int pThreadId, long pRecordCount, DataStorageCredentials pCredentials, long pStartId, long pEndPoint) { }
        public async virtual void RunCountQueries(DataStorageCredentials pCredentials) { }

        //TODO - placed here to be used later...can't be used in its current format :)
        protected source.Update Parse(source.SourceRecord sr)
        {
            source.Update u = null;

            if (sr != null && !string.IsNullOrEmpty(sr.Type) && !string.IsNullOrEmpty(sr.Data))
            {
                if (sr.Type.IndexOf(SourceDataConstants.TYPE_CLIENT_UPDATE_INDEX_OF_SRC_TERM) != -1)
                    u = Utilities.DeserializeSatelliteClient(sr.Data);
                else if (sr.Type.IndexOf(SourceDataConstants.TYPE_STATUS_UPDATE_INDEX_OF_SRC_TERM) != -1)
                    u = Utilities.DeserializeStatus(sr.Data);
                else
                    throw new System.Exception(Exceptions.ERR00000001);
            }

            return u;
        }

        protected void RunSqlQuery(string query, DataStorageCredentials pCredentials, string msg)
        {
            long recordCnt = 0;
            SqlDataReader rdr = null;
            SqlServerStorageCredentials credentials = (SqlServerStorageCredentials)pCredentials;
            SqlCommand cmd = Utilities.GetCommand(credentials);

            Console.WriteLine("Starting " + msg + "! " + DateTime.Now.ToString());

            try
            {
                cmd.CommandType = System.Data.CommandType.Text;
                cmd.CommandTimeout = 600000000;
                cmd.CommandText = query;

                rdr = cmd.ExecuteReader();

                if (rdr.Read())
                    recordCnt = Utilities.GetSafeLong(rdr[0]);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
            finally
            {
                Utilities.CloseDbObjects(cmd.Connection, cmd, rdr, null);
            }

            Console.WriteLine("Done with " + msg + "! Result: " + recordCnt.ToString() + DateTime.Now.ToString());
            Console.WriteLine("There were " + recordCnt.ToString() + " Inserted! " + DateTime.Now.ToString());
        }
    }
}
