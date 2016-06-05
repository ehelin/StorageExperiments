using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;

namespace Shared.dto.threading
{
    public class CreateLargeThreadJob
    {
        protected string sourceDir = string.Empty;
        protected string destinationFile = string.Empty;

        public CreateLargeThreadJob() { }

        public CreateLargeThreadJob(string sourceDir, string destinationFile)
        {
            this.sourceDir = sourceDir;
            this.destinationFile = destinationFile;
        }

        //HACK alert!! I tried spinning up the threads in a loop, but I got wierd results.  After reviewing a 
        //post (see below), I opted to go this route until I could find something less hackish.
        //http://stackoverflow.com/questions/30225476/task-run-with-parameters
        public virtual async void LaunchThreads(bool singleThread)
        {
            List<Task> tasks = new List<Task>();
            List<string> directoryFileNames = Utilities.GetDirectoryFileNames(sourceDir);

            if (singleThread)
            {
                tasks.Add(Task.Run(() => CreateLargeFile(1, this.sourceDir, destinationFile, true)));
            }
            else
            {
                tasks.Add(Task.Run(() => CreateLargeFile(1, directoryFileNames[0], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(2, directoryFileNames[1], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(3, directoryFileNames[2], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(4, directoryFileNames[3], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(5, directoryFileNames[4], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(6, directoryFileNames[5], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(7, directoryFileNames[6], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(8, directoryFileNames[7], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(9, directoryFileNames[8], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(10, directoryFileNames[9], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(11, directoryFileNames[10], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(12, directoryFileNames[11], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(13, directoryFileNames[12], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(14, directoryFileNames[13], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(15, directoryFileNames[14], destinationFile, false)));
                tasks.Add(Task.Run(() => CreateLargeFile(16, directoryFileNames[15], destinationFile, false)));
            }

            await Task.WhenAll(tasks);
        }

        protected async virtual void CreateLargeFile(int threadId, string sourceDir, string destinationFile, bool singleLargeFile) { }
    }
}
