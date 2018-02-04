using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace BatchEvents
{
    public class Function
    {
        IAmazonS3 S3Client { get; set; }

        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        public Function(IAmazonS3 s3Client)
        {
            this.S3Client = s3Client;
        }

        public async Task<string> FunctionHandler(S3Event evnt,
            ILambdaContext context)
        {
            var s3Event = evnt.Records?[0].S3;
            if (s3Event == null)
            {
                return null;
            }

            try
            {
                var keyName = s3Event.Object.Key;
                var bucketName = s3Event.Bucket.Name;
                context.Logger.LogLine(string.Format("New S3 object:{0} in the {1} bucket", keyName, bucketName));

                var tmpFilePath = "/tmp/" + keyName;
                await SaveS3ObjectToTmp(bucketName, keyName, tmpFilePath, context);
                context.Logger.LogLine("Downloaded file to tmp");

                var totalNumberOfEvents = File.ReadLines(tmpFilePath).Count() - 1;
                if (totalNumberOfEvents <= 0)
                {
                    context.Logger.LogLine("File has no events.  Nothing to batch here.");
                    return "Success";
                }

                var firstEvent = File.ReadLines(tmpFilePath).Take(2).ToList()[1];
                context.Logger.LogLine(firstEvent);
                var monitorKey = firstEvent.Split(',')[0];

                using (var dynamoClient = new AmazonDynamoDBClient())
                {
                    var tenantsTableName = Environment.GetEnvironmentVariable("Environment") + "-Tenants";
                    var dynamoResponse = await CheckIfTenantExists(context, monitorKey, tenantsTableName, dynamoClient);

                    if (dynamoResponse.Items.Count == 0)
                    {
                        await AddTenantToTable(context, dynamoClient, tenantsTableName, monitorKey);
                    }
                }

                const int batchSize = 10000;

                var numberOfFiles = Math.Ceiling((double) totalNumberOfEvents/batchSize);
                context.Logger.LogLine(string.Format("eventList.Count: {0}", totalNumberOfEvents));
                context.Logger.LogLine(string.Format("eventList.Count/batchSize: {0}", numberOfFiles));

                for (var i = 0; i < numberOfFiles; i++)
                {
                    var numberToTake = Math.Min(batchSize, totalNumberOfEvents - (i*batchSize));
                    var fileName = s3Event.Object.Key.Substring(0, s3Event.Object.Key.Length - 4) + "_" + i + ".csv";
                    var s3FilePath = monitorKey + "/" + fileName;
                    var targetBucketName = s3Event.Bucket.Name + "-batched";

                    context.Logger.LogLine("Getting " + numberToTake + " entries starting at " + i*batchSize);
                    var eventList = File.ReadLines(tmpFilePath).Skip(i*batchSize + 1).Take(numberToTake).ToList();
                    File.WriteAllLines("/tmp/" + fileName, eventList);

                    context.Logger.LogLine(string.Format("Uploading to bucket: {0}", targetBucketName));
                    var fileTransferUtility = new TransferUtility(new AmazonS3Client());
                    fileTransferUtility.Upload("/tmp/" + fileName, targetBucketName, s3FilePath);

                    context.Logger.LogLine("Successfully created new object in -batched S3 bucket");
                }

                context.Logger.LogLine("Events batched and uploaded to S3");

                return "Success";
            }
            catch (Exception e)
            {
                context.Logger.LogLine($"Error getting object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}. Make sure they exist and your bucket is in the same region as this function.");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }

        private static async Task<QueryResponse> CheckIfTenantExists(ILambdaContext context,
            string monitorKey,
            string tenantsTableName,
            AmazonDynamoDBClient dynamoClient)
        {
            var request = new QueryRequest
            {
                TableName = tenantsTableName,
                KeyConditionExpression = "MONITOR_KEY = :monitor_key",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    {
                        ":monitor_key", new AttributeValue {S = monitorKey}
                    }
                }
            };

            context.Logger.LogLine("Querying tenants table for monitor key");
            return await dynamoClient.QueryAsync(request);
        }

        private static async Task AddTenantToTable(ILambdaContext context,
            AmazonDynamoDBClient dynamoClient,
            string tenantsTableName,
            string monitorKey)
        {
            context.Logger.LogLine("Monitor key " + monitorKey + " not found in Tenants table");
            var table = Table.LoadTable(dynamoClient, tenantsTableName);

            var item = new Document
            {
                ["MONITOR_KEY"] = monitorKey,
                ["NAME"] = "UNKNOWN",
                ["RECIPIENT_LIST"] = "",
                ["CREATED_DATE"] = DateTime.Today.ToString("MM-dd-yyyy", CultureInfo.InvariantCulture),
                ["NEEDS_MANUAL_REFRESH"] = DynamoDBBool.False
            };
            context.Logger.LogLine("Putting item into Tenants table");
            try
            {
                await table.PutItemAsync(item);
                context.Logger.LogLine("Successfully added tenant " + monitorKey + " to tenants table");
            }
            catch (Exception)
            {
                context.Logger.LogLine("FAILED to add tenant " + monitorKey + " to tenants table");
                throw;
            }
        }

        private async Task SaveS3ObjectToTmp(string bucketName, string objectKey, string filePath, ILambdaContext context)
        {
            context.Logger.LogLine("Saving " + objectKey + " from bucket " + bucketName + " to " + filePath);
            using (S3Client = new AmazonS3Client())
            {
                using (var response = await S3Client.GetObjectAsync(bucketName, objectKey))
                {
                    await response.WriteResponseStreamToFileAsync(filePath, false, CancellationToken.None);
                }
            }
            context.Logger.LogLine("File saved");
        }
    }
}
