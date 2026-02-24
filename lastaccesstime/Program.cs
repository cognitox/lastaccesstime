using System;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Core;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;

namespace BlobLastAccessAudit
{
    internal class Program
    {
        // Usage examples:
        // 1) Managed Identity / DefaultAzureCredential (recommended):
        //    dotnet run -- "https://<account>.blob.core.windows.net" "<container>" "--mi"
        //
        // 2) Connection string:
        //    dotnet run -- "<connection-string>" "<container>" "--cs"
        //
        // Optional:
        //    --prefix "<prefix>"    (limit to blobs under a virtual folder)
        //    --dryrun              (don’t actually change tiers)
        //    --cooldown "<n>"      (sleep n ms between tier changes to reduce throttling risk)
        static async Task<int> Main(string[] args)
        {
            //if (args.Length < 3)
            //{
            //    Console.WriteLine("Usage:");
            //    Console.WriteLine("  dotnet run -- <endpoint-or-connstring> <container> --mi|--cs [--prefix <prefix>] [--dryrun] [--cooldown <ms>]");
            //    return 1;
            //}

            string endpointOrConnString = "";//args[0];
            string containerName = "";//args[1];
            string mode = "--cs";//args[2];

            string? prefix = null;
            bool dryRun = false;
            int cooldownMs = 0;

            //for (int i = 3; i < args.Length; i++)
            //{
            //    if (args[i].Equals("--prefix", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
            //    {
            //        prefix = args[++i];
            //    }
            //    else if (args[i].Equals("--dryrun", StringComparison.OrdinalIgnoreCase))
            //    {
            //        dryRun = true;
            //    }
            //    else if (args[i].Equals("--cooldown", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length && int.TryParse(args[i + 1], out var ms))
            //    {
            //        cooldownMs = ms;
            //        i++;
            //    }
            //}

            BlobContainerClient containerClient;

            if (mode.Equals("--mi", StringComparison.OrdinalIgnoreCase))
            {
                // endpointOrConnString example: https://mystorageaccount.blob.core.windows.net
                Uri accountUri = new Uri(endpointOrConnString);

                TokenCredential credential = new DefaultAzureCredential();
                BlobServiceClient serviceClient = new BlobServiceClient(accountUri, credential);
                containerClient = serviceClient.GetBlobContainerClient(containerName);
            }
            else if (mode.Equals("--cs", StringComparison.OrdinalIgnoreCase))
            {
                // endpointOrConnString is a full connection string
                BlobServiceClient serviceClient = new BlobServiceClient(endpointOrConnString);
                containerClient = serviceClient.GetBlobContainerClient(containerName);
            }
            else
            {
                Console.WriteLine("Invalid auth mode. Use --mi or --cs");
                return 1;
            }

            Console.WriteLine($"Container: {containerClient.Name}");
            Console.WriteLine($"Prefix: {(prefix ?? "(none)")}");
            Console.WriteLine($"DryRun: {dryRun}");
            Console.WriteLine($"CooldownMs: {cooldownMs}");
            Console.WriteLine();

            int scanned = 0;
            int missingLastAccess = 0;
            int tierChanged = 0;
            int skipped = 0;
            int errors = 0;

            // We request metadata/details but the key check is done via GetProperties for each blob.
            await foreach (BlobItem blobItem in containerClient.GetBlobsAsync())
            {
                scanned++;

                string blobName = blobItem.Name;
                BlobClient blobClient = containerClient.GetBlobClient(blobName);

                try
                {
                    Response<BlobProperties> propsResponse = await blobClient.GetPropertiesAsync();
                    BlobProperties props = propsResponse.Value;

                    // Most reliable: raw header. Microsoft documents it as:
                    // x-ms-last-access-time ... header isn't returned if last access tracking policy is disabled.
                    // [1](https://learn.microsoft.com/en-us/rest/api/storageservices/get-blob-properties)
                    bool hasHeader = propsResponse.GetRawResponse().Headers.TryGetValue("x-ms-last-access-time", out string? lastAccessHeader);

                    // Optional: some SDK versions also surface it as a nullable property.
                    // (If your SDK version doesn’t have it, this will still compile because we don’t reference it directly.)
                    bool hasLastAccess = hasHeader && !string.IsNullOrWhiteSpace(lastAccessHeader);

                    // Basic guardrails: don’t tier change snapshots/versions if you don’t want to.
                    // (Remove these guards if you want to tier everything.)
                    //if (blobItem.IsPrefix == true)
                    //{
                    //    skipped++;
                    //    continue;
                    //}

                    if (hasLastAccess)
                    {
                        Console.WriteLine($"[OK] {blobName} | lastAccessHeader={lastAccessHeader}");
                        continue;
                    }

                    missingLastAccess++;
                    Console.WriteLine($"[MISSING] {blobName} | no x-ms-last-access-time header -> set tier COLD");

                    if (!dryRun)
                    {
                        // Set to Cold tier. This is supported for block blobs in standard accounts.
                        // If a blob is in Archive, setting Cold will trigger rehydration behavior.
                        await blobClient.SetAccessTierAsync(AccessTier.Cold);
                        tierChanged++;

                        if (cooldownMs > 0)
                            await Task.Delay(cooldownMs);
                    }
                }
                catch (RequestFailedException ex)
                {
                    errors++;
                    Console.WriteLine($"[ERROR] {blobName} | {ex.Status} {ex.ErrorCode} | {ex.Message}");
                }

                // progress indicator every 1000 blobs
                if (scanned % 1000 == 0)
                {
                    Console.WriteLine($"--- Progress: scanned={scanned}, missingLastAccess={missingLastAccess}, tierChanged={tierChanged}, skipped={skipped}, errors={errors} ---");
                }
            }

            Console.WriteLine();
            Console.WriteLine("Done.");
            Console.WriteLine($"Scanned: {scanned}");
            Console.WriteLine($"Missing LastAccess: {missingLastAccess}");
            Console.WriteLine($"Tier changed to Cold: {tierChanged}");
            Console.WriteLine($"Skipped: {skipped}");
            Console.WriteLine($"Errors: {errors}");

            return 0;
        }
    }
}