using System;
using System.ComponentModel;
using Microsoft.WindowsAzure.Storage.Blob;
using Shared.dto.source;
using Shared.dto.threading;
using Microsoft.WindowsAzure;
using Microsoft.ServiceBus.Messaging;
using Microsoft.ServiceBus;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Data.SqlClient;
using Newtonsoft.Json;

namespace Shared.dto.EventHub
{
    public class EventHubLoadThreadJob : ThreadJob
    {
        public EventHubLoadThreadJob() : base() { }

        public EventHubLoadThreadJob(ThreadCompletion pDone,
                                 DataStorageCredentials pCredentials,
                                 long recordCount,
                                 long startId,
                                 int threadId) : base(pDone, pCredentials, recordCount, startId, threadId)
        { }

        public override void DoWork(object sender, DoWorkEventArgs e)
        {
            int ctr = 0;

            Console.WriteLine("Thread " + threadId + " starting with " + this.recordCount.ToString() + " records! " + DateTime.Now.ToString());

            while (ctr <= this.recordCount)
            {
                SourceRecord sr = GetRecord(startId);

                if (sr == null)
                    break;

                SourceRecordEventHub srch = ConvertSourceRecord(sr);                
                InsertRecord(srch);

                startId++;
                ctr++;
            }

            Console.WriteLine("Thread " + threadId + " Done! " + DateTime.Now.ToString());
        }

        private SourceRecordEventHub ConvertSourceRecord(SourceRecord sr)
        {
            SourceRecordEventHub srch = new SourceRecordEventHub();

            srch.Created = sr.Created;
            srch.Data = sr.Data;
            srch.Id = sr.Id;
            srch.Type = sr.Type;

            return srch;
        }

        private void InsertRecord(Shared.dto.EventHub.SourceRecordEventHub u)
        {
            EventHubStorageCredentials ehsc = (EventHubStorageCredentials)Credentials;
            EventHubClient client = EventHubClient.CreateFromConnectionString(ehsc.eventHub, ehsc.eventHubName);
            var serializedString = JsonConvert.SerializeObject(u);

            EventData data = new EventData(System.Text.Encoding.UTF8.GetBytes(serializedString))
            {
                PartitionKey = "$Default"
            };

            data.Properties.Add("Type", "SatelliteUpdate");

            try
            {
                client.Send(data);
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                if (data != null)
                {
                    data.Dispose();
                    data = null;
                }

                if (client != null)
                {
                    client.Close();
                    client = null;
                }
            }
        }

        private SourceRecord GetRecord(long id)
        {
            SqlConnection conn = null;
            SqlCommand cmd = null;
            SqlDataReader rdr = null;
            SourceRecord record = null;
            bool cont = true;
            int tryCtr = 0;

            while (cont)
            {
                try
                {
                    conn = new SqlConnection(SourceDataConstants.DB_CONNECTION);
                    cmd = conn.CreateCommand();
                    cmd.CommandTimeout = SourceDataConstants.DB_TIMEOUT;
                    cmd.CommandText = SourceDataConstants.SQL_GET_RECORD_ID;
                    cmd.CommandType = System.Data.CommandType.Text;

                    cmd.Parameters.Add(new System.Data.SqlClient.SqlParameter("@id", id));

                    cmd.Connection.Open();

                    rdr = cmd.ExecuteReader();

                    if (rdr.Read())
                    {
                        record = new SourceRecord();

                        record.Id = Utilities.GetSafeInt(rdr[0]);
                        record.Type = Utilities.GetSafeString(rdr[1]);
                        record.Data = Utilities.GetSafeString(rdr[2]);
                        record.Created = Utilities.GetSafeDate(rdr[3]);
                    }

                    cont = false;
                }
                catch (Exception ex)
                {
                    if (tryCtr > 3)
                    {
                        throw ex;
                    }
                    else
                        tryCtr++;
                }
                finally
                {
                    Utilities.CloseDbObjects(conn, cmd, rdr, null);
                }
            }

            return record;
        }
    }
}

