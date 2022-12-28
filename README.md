# GA4GH TES on Azure

This project is the Microsoft Genomics supported Azure implementation of the [GA4GH Task Execution Service (TES)](https://github.com/ga4gh/task-execution-schemas).  The TES API is an effort to define a standardized schema and API for describing batch execution tasks. A task defines a set of input files, a set of (Docker) containers and commands to run, a set of output files, and some other logging and metadata.

In the future, a publicly-hosted Docker image, likely named `mcr.microsoft.com/ga4gh/tes` will be built from this project.

# Install TES on Azure

To install TES on Azure, you need to meet below pre-requirements:

+ Install [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli)
+ Install [Helm](https://helm.sh/docs/intro/install/)
+ An Azure subscription

## Install TES on Azure
1. **Linux and OS X only**: assign execute permissions to the file by running the following command on the terminal:<br/>
`chmod +x <fileName>`. Replace `<fileName>` with the correct name: `deploy--on-azure-linux` or `deploy-tes-on-azure-osx.app`
1. You must specify the following parameters:
   1. `SubscriptionId` (**required**)
      1.  This can be obtained by navigating to the [subscriptions blade in the Azure portal](https://portal.azure.com/#blade/Microsoft_Azure_Billing/SubscriptionsBlade)
   1. `RegionName` (**required**)
      1. Specifies the region you would like to use for your TES on Azure instance. To find a list of all available regions, run `az account list-locations` on the command line or in PowerShell and use the desired region's "name" property for `RegionName`.
   1. `MainIdentifierPrefix` (*optional*)
      1. This string will be used to prefix the name of your TES on Azure resource group and associated resources. If not specified, the default value of "coa" followed by random characters is used as a prefix for the resource group and all Azure resources created for your TES on Azure instance. After installation, you can search for your resources using the `MainIdentifierPrefix` value.<br/>
   1. `ResourceGroupName` (*optional*, **required** when you only have owner-level access of the *resource group*)
      1. Specifies the name of a pre-existing resource group that you wish to deploy into.
      1. Specifies the name of a pre-existing resource group that you wish to deploy into.
   1. `AzureName` (*optional*, **required** when you only deploy services on sovereign cloud like AzureChinaCloud)
   2. `HelmBinaryPath` (*optional*)
      1. Specify path of Helm binary to help install identify Helm location.

Run the following at the command line or terminal after navigating to where your executable is saved:
```
.\deploy-tes-on-azure.exe --SubscriptionId <Your subscription ID> --RegionName <Your region> --MainIdentifierPrefix <Your string>  
```

**Example:**
```
.\deploy-tes-on-azure.exe --SubscriptionId 00000000-0000-0000-0000-000000000000 --RegionName westus2 --MainIdentifierPrefix coa --HelmBinaryPath /opt/homebrew/bin/helm
```

**Sovereign Cloud deployment example:**
```
az cloud set -n AzureChinaCloud

.\deploy-tes-on-azure.exe --SubscriptionId 00000000-0000-0000-0000-000000000000 --RegionName chinanorth3 --MainIdentifierPrefix coa --HelmBinaryPath /opt/homebrew/bin/helm --AzureName AzureChinaCloud
```


## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
