using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Tes.RunnerCLI.Commands
{
    public class ProcessLauncher
    {
       private readonly string executableName = Process.GetCurrentProcess().ProcessName;

       public ProcessExecutionResult LaunchProcess(string[] args)
       {
           var process = new Process();
            process.StartInfo.FileName = executableName;
            process.StartInfo.Arguments = string.Join(" ", args);
            process.StartInfo.UseShellExecute = false;
            process.StartInfo.RedirectStandardOutput = true;
            process.Start();
            process.WaitForExit();

            return ToProcessExecutionResult(process);
       }

       private ProcessExecutionResult ToProcessExecutionResult(Process process)
       {
           return new ProcessExecutionResult(
               process.StandardOutput.ReadToEnd(),
               process.StandardError.ReadToEnd(),
               process.ExitCode);
       }
    }

    public record ProcessExecutionResult(string StandardOutput, string StandardError, int ExitCode)
    {
    }
}
