# Azure Batch Authentication

The plugin uses a CustomAzureCredential chain that prefers the use of AzureCliCredential, then falls back to a ManagedIdentityCredential, and finally, an EnvironmentCredential (service principal) to create and destroy Azure Batch resources. The caller must have Contributor permissions on the Azure Batch account for the plugin to work properly. If you are using the Azure Storage plugin you should also have the Storage Blob Data Contributor role for the storage account(s) you use. 

To run a Snakemake workflow using your azure identity you need to ensure you are logged in using the [Azure CLI](https://learn.microsoft.com/en-us/cli/azure/):

```
az login
```

If you are running Snakemake from a GitHub workflow, you can authenticate the GitHub runner [with a User-Assigned Managed Identity](https://docs.github.com/en/actions/deployment/security-hardening-your-deployments/configuring-openid-connect-in-azure), and grant that Managed Identity Contributor permissions to the Azure Batch Account. 

When using the [Snakemake storage plugin for azure](https://snakemake.github.io/snakemake-plugin-catalog/plugins/storage/azure.html), or if you have tasks that need access to the Azure Container Registry or other Azure resources, it is required to setup a user assigned managed identity with the executor. The Batch nodes will assume this identity at runtime, and you can grant them permissions to Azure resources using this identity. 

The most common role to grant the Managed Identity will be [Storage Blob Data Contributor Role](https://learn.microsoft.com/en-us/azure/role-based-access-control/built-in-roles/storage#storage-blob-data-contributor) for any storage account you want to read/write data from the Azure Batch nodes. 

# Setup

The following required parameters are used to setup the executor, the can either be passed with their environment variable or command line flag forms. 

| ENVIRONMENT_VAR | CLI_FLAG | REQUIRED |
| :--------------:|:--------:|:---------|
|SNAKEMAKE_AZURE_BATCH_ACCOUNT_URL| --azure-batch-account-url | True |
|SNAKEMAKE_AZURE_BATCH_SUBSCRIPTION_ID| --azure-batch-subscription-id | True |
|SNAKEMAKE_AZURE_BATCH_RESOURCE_GROUP_NAME| --azure-batch-resource-group-name | True |

The remaining options are described [above](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/azure-batch.html#usage).


# Example 

# Write the Snakefile

# Run the jobs on Azure Batch nodes!

Here I pass the required values via CLI flags as described above, but they can also be detected from their respective environment variables. The example shown below are dummy values:

```
snakemake -j1 --executor azure-batch \
    --azure-batch-account-url https://accountname.westus2.batch.azure.com \
    --azure-batch-subscription-id d2c845cd-4903-40da-b34c-a6fec7115e21 \
    --azure-batch-resource-group-name rg-batch-test
```


# Example with Azure Storage Backend


```
snakemake -j1 --executor azure-batch \
    --azure-batch-account-url https://accountname.westus2.batch.azure.com \
    --azure-batch-subscription-id d2c845cd-4903-40da-b34c-a6fec7115e21 \
    --azure-batch-resource-group-name rg-batch-test
    --default-storage-provider azure
    --default-storage-prefix 'az://account/container/path/'
```