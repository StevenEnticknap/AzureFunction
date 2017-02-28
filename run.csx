#r "System.Runtime"
#r "System.Threading.Tasks"
#r "Microsoft.WindowsAzure.Storage"
#r "Microsoft.Threading.Tasks.dll"

using System;
using System.Net;
using System.Threading.Tasks;
using System.Configuration;
//using Newtonsoft.Json;
using Microsoft.Azure;
//using Microsoft.Azure.Common;
using Microsoft.Azure.Management.DataFactories;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob; 
 
public static void Run(string myBlob, TraceWriter log)
{
    //// Read the application settings keys from the environment variables.

    string activeDirectoryEndpoint = GetEnvironmentVariable("activeDirectoryEndpoint");
    string resourceManagerEndpoint = GetEnvironmentVariable("resourceManagerEndpoint");
    string windowsManagementUri = GetEnvironmentVariable("windowsManagementUri");
    string subscriptionId = GetEnvironmentVariable("subscriptionId");
    string activeDirectoryTenantId = GetEnvironmentVariable("activeDirectoryTenantId");
    string clientId = GetEnvironmentVariable("clientId");
    string clientSecret = GetEnvironmentVariable("clientSecret");
    string resourceGroupName = GetEnvironmentVariable("resourceGroupName");
    string dataFactoryName = GetEnvironmentVariable("dataFactoryName");
    string pipelineName = GetEnvironmentVariable("pipelineName");

    // --------------------------------
    // Get a connection to the pipeline
    // --------------------------------
    var authenticationContext = new AuthenticationContext(activeDirectoryEndpoint + activeDirectoryTenantId);

    var credential = new ClientCredential(clientId: clientId, clientSecret: clientSecret);
    var result = authenticationContext.AcquireTokenAsync(resource: windowsManagementUri, clientCredential: credential).Result;

    if (result == null) throw new InvalidOperationException("Failed to obtain the JWT token");

    var token = result.AccessToken;

    var aadTokenCredentials = new TokenCloudCredentials(subscriptionId, token);

    var resourceManagerUri = new Uri(resourceManagerEndpoint);

    var client = new DataFactoryManagementClient(aadTokenCredentials, resourceManagerUri);

    var pl = client.Pipelines.Get(resourceGroupName, dataFactoryName, pipelineName);

    if (pl == null)
    {
        log.Info("Pipeline handle is null");
    }
    else
    {
        log.Info("Got pipeline handle");
    }

    // ----------------------------------------------
    // Move the inbound file to the processing folder
    // ----------------------------------------------
    log.Info("*** MOVING TO PROCESSING ***");

    string storageConnectionString = GetEnvironmentVariable("test_STORAGE"); // Get connection info for the inbound/processing container.
    string inboundContainerName = "inbound";

    // Retrieve storage account from connection string.
    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

    // Create the blob client for interacting with the blob service
    CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

    // Retrieve reference to the "inbound" container.
    CloudBlobContainer inboundContainer = blobClient.GetContainerReference(inboundContainerName);

    // Loop through all blobs in the container and move them
    foreach (IListBlobItem item in inboundContainer.ListBlobs(null, false))
    {
        if (item.GetType() == typeof(CloudBlockBlob))
        {
            CloudBlockBlob blob = (CloudBlockBlob)item;

            log.Info($"Block blob of length {blob.Properties.Length}: {blob.Uri.ToString()}");

            // Get a handle on the appropriate processing folder based on the blob name
            string processingContainerName = GetProcessingFolderNameFromBlob(blob.Name);

            if (processingContainerName != string.Empty)
            {
                // Retrieve reference to the "processing" container
                CloudBlobContainer processingContainer = blobClient.GetContainerReference(processingContainerName);

                CloudBlockBlob newBlob = null;

                // Get a reference to a destination blob (in this case, a new blob).
                newBlob = processingContainer.GetBlockBlobReference(blob.Name);

                if (blob.Exists())
                {
                    // Get the ID of the copy operation.
                    string copyId = newBlob.StartCopy(blob).ToString();

                    // Fetch the destination blob's properties before checking the copy state.
                    newBlob.FetchAttributes();

                    // If the copy worked, delete original blob from inbound folder
                    if (newBlob.Exists())
                    {
                        blob.DeleteIfExists(DeleteSnapshotsOption.IncludeSnapshots, null, null, null);
                    }
                }

                log.Info($"Copied and moved blob {blob.Uri.ToString()}");
            }
        }
    }

    // Re-schedule pipeline for a configurable time in the future
    try
    {
        // TODO: read the "wait interval" from configuration.
        DateTime slice = DateTime.Now.AddMinutes(2);

        slice = DateTime.SpecifyKind(slice, DateTimeKind.Utc);
        log.Info($"New Start: {slice}");

        DateTime endSlice = slice.AddMinutes(15); // The slice will end fifteen minutes later.

        //pl.Pipeline.Properties.Start = DateTime.Parse($"{slice.Date:yyyy-MM-dd}T01:00:00Z");
        //pl.Pipeline.Properties.End = DateTime.Parse($"{slice.Date:yyyy-MM-dd}T01:00:00Z");
        //pl.Pipeline.Properties.Start = DateTime.Parse($"{slice.Date:yyyy-MM-dd}T{slice.TimeOfDay}");
        pl.Pipeline.Properties.Start = DateTime.Parse($"{slice.Date:yyyy-MM-dd}T01:00:00Z");
        //log.Info($"New start time: {pl.Pipeline.Properties.Start}");

        pl.Pipeline.Properties.End = DateTime.Parse($"{endSlice.Date:yyyy-MM-dd}T01:00:00Z");
        pl.Pipeline.Properties.IsPaused = false;
        
        client.Pipelines.CreateOrUpdate(resourceGroupName, dataFactoryName, new PipelineCreateOrUpdateParameters()
        {
            Pipeline = pl.Pipeline
        });

        log.Info($"Pipeline rescheduled for {slice}");

    }
    catch (Exception e)
    {
        log.Info(e.Message);
    }

    log.Info($"C# Blob trigger function processed");
}

public static string GetEnvironmentVariable(string name)
{
    return System.Environment.GetEnvironmentVariable(name, EnvironmentVariableTarget.Process);
}

public static string GetProcessingFolderNameFromBlob(string blobName)
{
    // TODO: read this from configuration/app settings
    string folderName = "";

    if (blobName.Contains("FileOneType"))
    {
        folderName = "processingfileone";
    }
    else if (blobName.Contains("FileTwoType"))
	{
		folderName = "processingfiletwo"
	}

    return folderName;
}