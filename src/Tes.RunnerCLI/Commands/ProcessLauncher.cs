using System.Diagnostics;
using System.Reflection;

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

        private static void ConfigureProcessStartInfoForMethodInvocation(MethodInfo method, string[] args, ProcessStartInfo psi)
        {
            if (method.ReturnType != typeof(void) &&
                method.ReturnType != typeof(int) &&
                method.ReturnType != typeof(Task<int>))
            {
                throw new ArgumentException("method has an invalid return type", nameof(method));
            }
            if (method.GetParameters().Length > 1)
            {
                throw new ArgumentException("method has more than one argument argument", nameof(method));
            }
            if (method.GetParameters().Length == 1 && method.GetParameters()[0].ParameterType != typeof(string[]))
            {
                throw new ArgumentException("method has non string[] argument", nameof(method));
            }

            // If we need the host (if it exists), use it, otherwise target the console app directly.
            Type t = method.DeclaringType;
            Assembly a = t.GetTypeInfo().Assembly;

            bool enableDebuggerAttach = Debugger.IsAttached && Environment.OSVersion.Platform == PlatformID.Win32NT;
            int pid = Process.GetCurrentProcess().Id;

            //string programArgs = PasteArguments.Paste(new string[] { a.FullName, t.FullName, method.Name, enableDebuggerAttach.ToString(System.Globalization.CultureInfo.InvariantCulture), pid.ToString(System.Globalization.CultureInfo.InvariantCulture) });
            //string functionArgs = PasteArguments.Paste(args);
            //string fullArgs = HostArguments + " " + " " + programArgs + " " + functionArgs;

            //psi.FileName = HostFilename;
            //psi.Arguments = fullArgs;
        }
    }

    public record ProcessExecutionResult(string StandardOutput, string StandardError, int ExitCode)
    {
    }
}
