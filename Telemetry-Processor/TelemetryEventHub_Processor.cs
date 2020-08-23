using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace Solution_Accelerator
{
    public static class TelemetryEventHub_Processor
    {
        [FunctionName("TelemetryEventHub_Processor")]
        public static async Task Run([EventHubTrigger("devicetelemetryhub", ConsumerGroup = "devicetelemetrytofunction", Connection = "EVENTHUB_CS")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"C# Event Hub trigger function processed a message: {messageBody}");
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();

            //var exceptions = new List<Exception>();

            //foreach (EventData ed in eventData)
            //{
            //    try
            //    {
            //        if (ed.SystemProperties.ContainsKey("iothub-message-source"))
            //        {
            //            string deviceId = ed.SystemProperties["iothub-connection-device-id"].ToString();
            //            string msgSource = ed.SystemProperties["iothub-message-source"].ToString();
            //            string signalr_target = string.Empty;
            //            string model_id = string.Empty;

            //            log.LogInformation($"Telemetry Source  : {msgSource}");
            //            log.LogInformation($"Telemetry Message : {Encoding.UTF8.GetString(ed.Body.Array, ed.Body.Offset, ed.Body.Count)}");

            //            DateTime enqueuTime = (DateTime)ed.SystemProperties["iothub-enqueuedtime"];

            //            if (ed.SystemProperties.ContainsKey("dt-dataschema"))
            //            {
            //                model_id = ed.SystemProperties["dt-dataschema"].ToString();
            //            }

            //            NOTIFICATION_DATA signalrData = new NOTIFICATION_DATA
            //            {
            //                eventId = ed.SystemProperties["x-opt-sequence-number"].ToString(),
            //                eventType = "Event Hubs",
            //                eventTime = enqueuTime.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"),
            //                eventSource = msgSource,
            //                deviceId = deviceId,
            //                dtDataSchema = model_id,
            //            };

            //            // Process telemetry based on message source
            //            switch (msgSource)
            //            {
            //                case "Telemetry":
            //                    OnTelemetryReceived(signalrData, ed, log);
            //                    signalr_target = "DeviceTelemetry";
            //                    break;
            //                case "twinChangeEvents":
            //                    OnDeviceTwinChanged(signalrData, ed, log);
            //                    signalr_target = "DeviceTwinChange";
            //                    break;
            //                case "digitalTwinChangeEvents":
            //                    OnDigitalTwinTwinChanged(signalrData, ed, log);
            //                    signalr_target = "DigitalTwinChange";
            //                    break;
            //                case "deviceLifecycleEvents":
            //                    OnDeviceLifecycleChanged(signalrData, ed, log);
            //                    signalr_target = "DeviceLifecycle";
            //                    break;
            //                default:
            //                    break;
            //            }

            //            var data = JsonConvert.SerializeObject(signalrData);

            //            await signalRMessage.AddAsync(new SignalRMessage
            //            {
            //                Target = signalr_target,
            //                Arguments = new[] { data }
            //            });

            //        }
            //        else
            //        {
            //            log.LogInformation("Unknown Message Source");
            //        }
            //    }
            //    catch (Exception e)
            //    {
            //        exceptions.Add(e);
            //    }
            //}

            //if (exceptions.Count > 1)
            //    throw new AggregateException(exceptions);

            //if (exceptions.Count == 1)
            //    throw exceptions.Single();
        }

        // Placeholder to process specific for event types
        private static void OnTelemetryReceived(NOTIFICATION_DATA signalrData, EventData eventData, ILogger log)
        {
            log.LogInformation($"OnTelemetryReceived");
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

        }

        private static void OnDeviceTwinChanged(NOTIFICATION_DATA signalrData, EventData eventData, ILogger log)
        {
            log.LogInformation($"OnDeviceTwinChanged");
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
        }

        private static void OnDigitalTwinTwinChanged(NOTIFICATION_DATA signalrData, EventData eventData, ILogger log)
        {
            log.LogInformation($"OnDigitalTwinTwinChanged");
            signalrData.data = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);
        }

        private static void OnDeviceLifecycleChanged(NOTIFICATION_DATA signalrData, EventData eventData, ILogger log)
        {
            log.LogInformation($"OnDeviceLifecycleChanged");
            signalrData.data = JsonConvert.SerializeObject(eventData.Properties);
        }

        public class NOTIFICATION_DATA
        {
            public string eventId { get; set; }
            public string eventType { get; set; }
            public string deviceId { get; set; }
            public string eventSource { get; set; }
            public string eventTime { get; set; }
            public string data { get; set; }
            public string dtDataSchema { get; set; }
        }
    }
}
