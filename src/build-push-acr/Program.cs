// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// See https://aka.ms/new-console-template for more information

using System.CommandLine;
using System.CommandLine.Builder;
using System.CommandLine.Parsing;
using Azure.Containers.ContainerRegistry;
using Azure.Identity;
using Azure.ResourceManager.ContainerRegistry;
using BuildPushAcr;
using CommonUtilities.AzureCloud;

Option<Version> tagOption = new(["--tag", "-v"], parseArgument: result => new(result.Tokens.Single().Value), description: "The tag to fetch from the repository.") { IsRequired = true, Arity = ArgumentArity.ExactlyOne };

Command githubCommand = new("github", "Builds image(s) from the GitHub repository.")
{
    tagOption
};

var directoryOption = new Option<DirectoryInfo>(["--directory", "-d"], "The directory containing the solution. Usually is the root of the respository.") { IsRequired = true, Arity = ArgumentArity.ExactlyOne }
    .ExistingOnly();

Command localCommand = new("local", "Builds image(s) from a local directory.")
{
    directoryOption
};

RootCommand command = new("Pushes source code to ACR to build into image(s).")
{
    githubCommand,
    localCommand
};

static Option<T> AddGlobalOption<T>(Command command, Option<T> option)
{
    command.AddGlobalOption(option);
    return option;
}

var cloudNameOption = AddGlobalOption(command, new Option<string>(["--cloud-name", "-c"], "The cloud name to use.") { IsRequired = true, Arity = ArgumentArity.ExactlyOne })
    .FromAmong([nameof(Azure.ResourceManager.ArmEnvironment.AzurePublicCloud), nameof(Azure.ResourceManager.ArmEnvironment.AzureGovernment), nameof(Azure.ResourceManager.ArmEnvironment.AzureChina)]);
var buildTypeOption = AddGlobalOption(command, new Option<BuildType>(["--build-type", "-t"], "The type of build to perform.") { IsRequired = true, Arity = ArgumentArity.ExactlyOne });
var subscriptionOption = AddGlobalOption(command, new Option<string>(["--subscription", "-s"], "The subscription ID to use.") { IsRequired = true, Arity = ArgumentArity.ExactlyOne });
var resourceGroupOption = AddGlobalOption(command, new Option<string>(["--resource-group", "-g"], "The resource group to use.") { IsRequired = true, Arity = ArgumentArity.ExactlyOne });
var registryOption = AddGlobalOption(command, new Option<string>(["--registry", "-r"], "The registry to use.") { IsRequired = true, Arity = ArgumentArity.ExactlyOne });
var noLogsOption = AddGlobalOption(command, new Option<bool>("--no-logs", "Do not display logs.") { IsRequired = false, Arity = ArgumentArity.ZeroOrOne });

async Task Handler(System.CommandLine.Invocation.InvocationContext context, Func<System.CommandLine.Invocation.InvocationContext, IArchive> getTar)
{
    var cancellationToken = context.GetCancellationToken();
    var azureCloud = await AzureCloudConfig.FromKnownCloudNameAsync(cloudName: context.ParseResult.GetValueForOption(cloudNameOption)!, retryPolicyOptions: Microsoft.Extensions.Options.Options.Create<CommonUtilities.Options.RetryPolicyOptions>(new()));
    var buildType = context.ParseResult.GetValueForOption(buildTypeOption);
    var subscription = context.ParseResult.GetValueForOption(subscriptionOption);
    var resourceGroup = context.ParseResult.GetValueForOption(resourceGroupOption);
    var registry = context.ParseResult.GetValueForOption(registryOption);

    LogType logType;

    if (context.ParseResult.HasOption(noLogsOption) && context.ParseResult.GetValueForOption(noLogsOption))
    {
        logType = LogType.None;
    }
    else if (Console.IsOutputRedirected)
    {
        logType = LogType.NonInteractive;
    }
    else
    {
        logType = LogType.Interactive;
    }

    AcrBuild acrBuild;
    IAsyncDisposable? tarDisposable = default;

    try
    {
        var tar = getTar(context);
        tarDisposable = tar as IAsyncDisposable;

        acrBuild = new(buildType: buildType,
            tag: await tar.GetTagAsync(cancellationToken),
            acrId: ContainerRegistryResource.CreateResourceIdentifier(context.ParseResult.GetValueForOption(subscriptionOption), context.ParseResult.GetValueForOption(resourceGroupOption), context.ParseResult.GetValueForOption(registryOption)),
            credential: new AzureCliCredential(new() { AuthorityHost = azureCloud.AuthorityHost }),
            audience: new ContainerRegistryAudience(azureCloud.ArmEnvironment!.Value.Endpoint.AbsoluteUri));

        await acrBuild.LoadAsync(tar, azureCloud.ArmEnvironment.Value, cancellationToken);
    }
    finally
    {
        await (tarDisposable?.DisposeAsync() ?? ValueTask.CompletedTask);
    }

    _ = await acrBuild.BuildAsync(logType, cancellationToken);
    Console.WriteLine($"Image tag: {acrBuild.Tag}");
}

IArchive GetLocalGitArchive(System.CommandLine.Invocation.InvocationContext context)
    => AcrBuild.GetLocalGitArchiveAsync(context.ParseResult.GetValueForOption(directoryOption)!);

IArchive GetGitHubArchive(System.CommandLine.Invocation.InvocationContext context)
    => AcrBuild.GetGitHubArchive(context.ParseResult.GetValueForOption(buildTypeOption),
        context.ParseResult.GetValueForOption(tagOption)!.ToString(3),
        GitHubArchive.GetAccessTokenProvider());

githubCommand.SetHandler(context => Handler(context, GetGitHubArchive));
localCommand.SetHandler(context => Handler(context, GetLocalGitArchive));
command.SetHandler(() => throw new InvalidOperationException("No command specified."));

return await new CommandLineBuilder(command)
    .UseDefaults()
    .EnableLegacyDoubleDashBehavior()
    .EnablePosixBundling()
    .Build()
    .InvokeAsync(args);

//var tar = await AcrBuild.GetLocalGitArchiveAsync(new DirectoryInfo(@"D:\source\repos\alt\ga4gh-tes"), CancellationToken.None);

//AcrBuild acrBuild = new(buildType, /*new("5.5.1")*/await tar.GetTagAsync(CancellationToken.None),
//    //ContainerRegistryResource.CreateResourceIdentifier("f483a450-5f19-4b20-9326-b5852bb89d83", "coa-integration-test", "coaintegrationtest"),
//    ContainerRegistryResource.CreateResourceIdentifier("594bef42-33f3-42df-b056-e7399d6ae7a0", "bm-coa-tests", "bmtestacr"),
//    new AzureCliCredential(new() { AuthorityHost = AzureAuthorityHosts.AzurePublicCloud }),
//    ContainerRegistryAudience.AzureResourceManagerPublicCloud);

//{
//    //using var tar = AcrBuild.GetGitHubArchive(buildType, "bmurri/debug-delay");
//    //using var tar = AcrBuild.GetGitHubArchive(buildType, "c60cc08"); // TES
//    //using var tar = AcrBuild.GetGitHubArchive(buildType, "ad6d701"); // COA
//    await acrBuild.LoadAsync(tar, CancellationToken.None);
//}

//await acrBuild.BuildAsync(LogType.Interactive, CancellationToken.None);
//Console.WriteLine($"Image tag: {acrBuild.Tag}");
