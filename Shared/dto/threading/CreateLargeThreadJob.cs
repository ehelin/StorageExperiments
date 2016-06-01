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
        public virtual async void LaunchThreads()
        {
            List<Task> tasks = new List<Task>();

            tasks.Add(Task.Run(() => CreateLargeFile(1, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(2, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(3, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(4, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(5, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(6, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(7, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(8, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(9, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(10, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(11, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(12, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(13, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(14, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(15, sourceDir, destinationFile)));
            tasks.Add(Task.Run(() => CreateLargeFile(16, sourceDir, destinationFile)));

            await Task.WhenAll(tasks);
        }

        protected async virtual void CreateLargeFile(int threadId, string sourceDir, string destinationFile) { }
    }
}
