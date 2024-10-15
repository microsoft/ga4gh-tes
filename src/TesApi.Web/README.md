# ASP.NET Core Docker Production Sample

This ASP.NET Core Docker sample demonstrates a best practice pattern for building Docker images for ASP.NET Core apps for production. The sample works with both Linux and Windows containers.

The [sample Dockerfile](Dockerfile) creates an ASP.NET Core application Docker image based off of the [ASP.NET Core Runtime Docker image](https://hub.docker.com/r/microsoft/aspnetcore/).

It uses the [Docker multi-stage build feature](https://github.com/dotnet/announcements/issues/18) to build the sample in a container based on the larger [ASP.NET Core Build Docker image](https://hub.docker.com/r/microsoft/aspnetcore-build/) and then copies the final build result into a Docker image based on the smaller [ASP.NET Core Docker Runtime image](https://hub.docker.com/r/microsoft/aspnetcore/). The build image contains tools that are required to build applications while the runtime image does not.

This sample requires [Docker 17.06](https://docs.docker.com/release-notes/docker-ce) or later of the [Docker client](https://www.docker.com/products/docker). You need the latest Windows 10 or Windows Server 2016 to use [Windows containers](http://aka.ms/windowscontainers). The instructions assume you have the [Git](https://git-scm.com/downloads) client installed.

## Getting the sample

The easiest way to get the sample is by cloning the samples repository with git, using the following instructions.

```console
git clone https://github.com/dotnet/dotnet-docker-samples/
```

You can also [download the repository as a zip](https://github.com/dotnet/dotnet-docker-samples/archive/master.zip).

## Build and run the sample with Docker for Linux containers

You can build and run the sample in Docker using Linux containers using the following commands. The instructions assume that you are in the root of the repository.

```console
cd tesapi
docker build -t tesapi .
docker run -it --rm -p 8000:80 tesapi
```

After the application starts, visit `http://localhost:8000` in your web browser.

Note: The `-p` argument maps port 8000 on you local machine to port 80 in the container (the form of the port mapping is `host:container`). See the [Docker run reference](https://docs.docker.com/engine/reference/commandline/run/) for more information on commandline paramaters.

## Build and run the sample with Docker for Windows containers

You can build and run the sample in Docker using Windows containers using the following commands. The instructions assume that you are in the root of the repository.

```console
cd tesapi
docker build -t tesapi .
docker run -it --rm --name aspnetcore_sample tesapi
```

You must navigate to the container IP (as opposed to http://localhost) in your browser directly when using Windows containers. You can get the IP address of your container with the following steps:

1. Open up another command prompt.
1. Run `docker ps` to see your running containers. The "aspnetcore_sample" container should be there.
1. Run `docker exec aspnetcore_sample ipconfig`.
1. Copy the container IP address and paste into your browser (for example, `172.29.245.43`).

See the following example of how to get the IP address of a running Windows container.

```console
C:\git\dotnet-docker-samples\tesapi>docker exec aspnetcore_sample ipconfig

Windows IP Configuration


Ethernet adapter Ethernet:

   Connection-specific DNS Suffix  . : contoso.com
   Link-local IPv6 Address . . . . . : fe80::1967:6598:124:cfa3%4
   IPv4 Address. . . . . . . . . . . : 172.29.245.43
   Subnet Mask . . . . . . . . . . . : 255.255.240.0
   Default Gateway . . . . . . . . . : 172.29.240.1
```

Note: [`docker exec`](https://docs.docker.com/engine/reference/commandline/exec/) supports identifying containers with name or hash. The name is used above. It runs a new command (as opposed to the [entrypoint](https://docs.docker.com/engine/reference/builder/#entrypoint)) in a running container.

Some people prefer using `docker inspect` for this same purpose. See the following example, below:

```console
C:\git\dotnet-docker-samples\tesapi>docker inspect -f "{{ .NetworkSettings.Networks.nat.IPAddress }}" aspnetcore_sample
172.25.157.148
```

## Build and run the sample locally

You can build and run the sample locally with the [.NET Core 2.0 SDK](https://www.microsoft.com/net/download/core) using the following commands. The commands assume that you are in the root of the repository.

```console
cd tesapi
dotnet run
```

After the application starts, visit `http://localhost:8000` in your web browser.

You can produce an application that is ready to deploy to production locally using the following command.

```console
dotnet publish -c release -o out
```

You can run the application on **Windows** using the following command.

```console
dotnet out\tesapi.dll
```

You can run the application on **Linux or macOS** using the following command.

```console
dotnet out/tesapi.dll
```

Note: The `-c release` argument builds the application in release mode (the default is debug mode). See the [dotnet run reference](https://docs.microsoft.com/dotnet/core/tools/dotnet-run) for more information on commandline parameters.

## Docker Images used in this sample

The following Docker images are used in this sample

* [microsoft/aspnetcore-build:2.0](https://hub.docker.com/r/microsoft/aspnetcore-build)
* [microsoft/aspnetcore:2.0](https://hub.docker.com/r/microsoft/aspnetcore/)

## Related Resources

* [ASP.NET Core Getting Started Tutorials](https://www.asp.net/get-started)
* [.NET Core Production Docker sample](../dotnetapp-prod/README.md)
* [.NET Core Docker samples](../README.md)
* [.NET Framework Docker samples](https://github.com/Microsoft/dotnet-framework-docker-samples)
