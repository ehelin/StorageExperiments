using System.Collections.Generic;
using System.ComponentModel;

namespace Shared.dto.threading
{
    public class ThreadJob
    {
        protected DataStorageCredentials Credentials { get; set; }
        protected IList<ThreadCompletion> theadDone = null;
        protected BackgroundWorker worker = null;
        protected ThreadCompletion done = null;
        protected long recordCount = 0;
        protected long startId = 0;
        protected int threadId = 0;

        public ThreadJob() { }

        public ThreadJob(ThreadCompletion pDone,
                        DataStorageCredentials pCredentials,
                        long pRecordCount,
                        long pStartId,
                        int pThreadId)
        {
            this.done = pDone;
            this.Credentials = pCredentials;
            this.recordCount = pRecordCount;
            this.startId = pStartId;
            this.threadId = pThreadId;

            worker = new BackgroundWorker { WorkerReportsProgress = true };
            worker.DoWork += DoWork;
            worker.RunWorkerCompleted += bw_RunWorkerCompleted;
        }

        public void Execute()
        {
            worker.RunWorkerAsync();
        }
        public virtual void DoWork(object sender, DoWorkEventArgs e) { }
        public void bw_RunWorkerCompleted(object sender, RunWorkerCompletedEventArgs e)
        {
            done.ThreadDone = true;
        }
        
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
    }
}
