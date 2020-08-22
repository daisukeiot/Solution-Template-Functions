using Azure.Core.Pipeline;
using Azure.DigitalTwins.Core;
using Azure.Identity;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.EventGrid;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Net.Http;
using System.Threading.Tasks;


namespace Solution_Accelerator
{
    public class ADTEventToDT_Processor
    {
        const string adtAppId = "https://digitaltwins.azure.net";
        private static readonly string adtInstanceUrl = Environment.GetEnvironmentVariable("ADT_SERVICE_URL");
        private static readonly HttpClient httpClient = new HttpClient();

        [FunctionName("ADTEventToDT_Processor")]
        public static async Task Run([EventGridTrigger] EventGridEvent eventGridEvent, ILogger log)
        {
            log.LogInformation("Start execution");

            DigitalTwinsClient client;
            try
            {
                ManagedIdentityCredential cred = new ManagedIdentityCredential(adtAppId);
                client = new DigitalTwinsClient(new Uri(adtInstanceUrl), cred, new DigitalTwinsClientOptions { Transport = new HttpClientTransport(httpClient) });
                log.LogInformation("ADT service client connection created.");
            }
            catch (Exception e)
            {
                log.LogError($"ADT service client connection failed. {e}");
                return;
            }

            if (client != null)
            {
                try
                {
                    if (eventGridEvent != null && eventGridEvent.Data != null)
                    {
                        string twinId = eventGridEvent.Subject.ToString();
                        JObject message = (JObject)JsonConvert.DeserializeObject(eventGridEvent.Data.ToString());

                        log.LogInformation($"Reading event from {twinId}: {eventGridEvent.EventType}: {message["data"]}");

                        //Find and update parent Twin
                        string parentId = await AdtUtilities.FindParentAsync(client, twinId, "contains", log);
                        if (parentId != null)
                        {
                            // Read properties which values have been changed in each operation
                            foreach (var operation in message["data"]["patch"])
                            {
                                string opValue = (string)operation["op"];
                                if (opValue.Equals("replace"))
                                {
                                    string propertyPath = ((string)operation["path"]);

                                    if (propertyPath.Equals("/Temperature"))
                                    {
                                        await AdtUtilities.UpdateTwinPropertyAsync(client, parentId, propertyPath, operation["value"].Value<float>(), log);
                                    }
                                }
                            }
                        }

                    }
                }
                catch (Exception e)
                {
                    log.LogError(e.ToString());
                }
            }
        }
    }
}
