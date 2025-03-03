// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.CommandLine;
using System.CommandLine.IO;
using System.CommandLine.Rendering;
using System.Text;

namespace GenerateBatchVmSkus
{
    internal static class ConsoleHelper
    {
        internal static IConsole? Console;

        private static readonly object consoleLock = new();
        private static string linePrefix = string.Empty;

        private static void ThrowIfNotInitialized()
        {
            if (Console is null)
            {
                throw new InvalidOperationException($"{nameof(ConsoleHelper)} is not initialized.");
            }
        }

        internal static void WriteLine(string? content = null)
        {
            ThrowIfNotInitialized();

            lock (consoleLock)
            {
                Console!.Out.WriteLine(linePrefix + content ?? string.Empty);
                linePrefix = string.Empty;
            }
        }

        internal static void WriteLine(ForegroundColorSpan? foreground, string? content = null)
        {
            ThrowIfNotInitialized();

            lock (consoleLock)
            {
                var terminal = Console.GetTerminal();
                terminal?.Render(foreground ?? TextSpan.Empty());
                Console!.Out.WriteLine(linePrefix + content ?? string.Empty);
                linePrefix = string.Empty;
                terminal?.ResetColor();
            }
        }

        internal static void WriteLine(string name, ForegroundColorSpan? foreground, string? content = null)
        {
            ThrowIfNotInitialized();

            var result = string.Empty;

            if (string.IsNullOrEmpty(content))
            {
                result = Environment.NewLine;
            }
            else
            {
                var lines = content
                    .ReplaceLineEndings(Environment.NewLine)
                    .Split(Environment.NewLine);

                if (string.IsNullOrEmpty(lines.Last()))
                {
                    lines = [.. lines.SkipLast(1)];
                }

                result = string.Join(Environment.NewLine,
                    lines.Select((line, i) => (i == 0 ? $"[{name}]: " : @"    ") + line));
            }

            WriteLine(foreground, result);
        }

        internal static void WriteTemporaryLine(Func<string> getLine)
        {
            ThrowIfNotInitialized();

            if (!Console!.IsOutputRedirected)
            {
                lock (consoleLock)
                {
                    var line = getLine();
                    StringBuilder sb = new(linePrefix);
                    sb.Append(line);
                    sb.Append('\r');
                    Console.Out.Write(sb.ToString());

                    sb.Clear();
                    sb.Append([.. Enumerable.Repeat(' ', line.Length)]);
                    sb.Append('\r');
                    linePrefix = sb.ToString();
                }
            }
        }
    }
}
