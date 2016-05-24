using System.Collections.Generic;
using System.Threading.Tasks;

namespace Shared.dto.threading
{
    public class DownloadThreadJob
    {
        protected DataStorageCredentials credentials;
        protected List<string> fileListPath;

        public DownloadThreadJob() { }

        public DownloadThreadJob(DataStorageCredentials pCredentials, List<string> fileListPath)
        {
            this.credentials = pCredentials;
            this.fileListPath = fileListPath;
        }

        //HACK alert!! I tried spinning up the threads in a loop, but I got wierd results.  After reviewing a 
        //post (see below), I opted to go this route until I could find something less hackish.
        //http://stackoverflow.com/questions/30225476/task-run-with-parameters
        public virtual async void LaunchThreads()
        {
            List<Task> tasks = new List<Task>();
            
            tasks.Add(Task.Run(() => Download(credentials, 1, fileListPath[0])));
            tasks.Add(Task.Run(() => Download(credentials, 2, fileListPath[1])));
            tasks.Add(Task.Run(() => Download(credentials, 3, fileListPath[2])));
            tasks.Add(Task.Run(() => Download(credentials, 4, fileListPath[3])));
            tasks.Add(Task.Run(() => Download(credentials, 5, fileListPath[4])));
            tasks.Add(Task.Run(() => Download(credentials, 6, fileListPath[5])));
            tasks.Add(Task.Run(() => Download(credentials, 7, fileListPath[6])));
            tasks.Add(Task.Run(() => Download(credentials, 8, fileListPath[7])));
            tasks.Add(Task.Run(() => Download(credentials, 9, fileListPath[8])));
            tasks.Add(Task.Run(() => Download(credentials, 10, fileListPath[9])));
            tasks.Add(Task.Run(() => Download(credentials, 11, fileListPath[10])));
            tasks.Add(Task.Run(() => Download(credentials, 12, fileListPath[11])));
            tasks.Add(Task.Run(() => Download(credentials, 13, fileListPath[12])));
            tasks.Add(Task.Run(() => Download(credentials, 14, fileListPath[13])));
            tasks.Add(Task.Run(() => Download(credentials, 15, fileListPath[14])));
            tasks.Add(Task.Run(() => Download(credentials, 16, fileListPath[15])));

            await Task.WhenAll(tasks);
        }

        protected async virtual void Download(DataStorageCredentials credentials, int threadId, string fileListPath) { }
    }
}
