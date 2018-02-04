using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.SQS;
using Amazon.SQS.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace GenerateAccountDailyMetrics
{
    public class Function
    {
        public async Task<string> FunctionHandler(ILambdaContext context)
        {
            try
            {
                context.Logger.LogLine("Entering function");

                var newMetricsTasksQueueUrl = Environment.GetEnvironmentVariable("AccountDailyMetricsTasksQueueUrl");

                var receiveMessageRequest = new ReceiveMessageRequest
                {
                    QueueUrl = newMetricsTasksQueueUrl
                };

                using (var sqsClient = new AmazonSQSClient())
                {
                    var pollForMoreMessages = true;
                    while (pollForMoreMessages && context.RemainingTime > new TimeSpan(0, 0, 5))
                    {
                        context.Logger.LogLine(string.Format("Looking for messages on the queue.  Remaining time: {0} min {1} seconds", context.RemainingTime.Minutes, context.RemainingTime.Seconds));
                        var receiveMessageResponse = await sqsClient.ReceiveMessageAsync(receiveMessageRequest);
                        pollForMoreMessages = receiveMessageResponse.Messages.Count > 0;
                        context.Logger.LogLine(string.Format("Messages retrieved: {0}", receiveMessageResponse.Messages.Count));

                        if (!receiveMessageResponse.Messages.Any()) continue;

                        var message = receiveMessageResponse.Messages.FirstOrDefault();
                        var messageParts = message.Body.Split(',');
                        var monitorKey = messageParts[0].Split(':')[1];
                        var dateToQuery = messageParts[1].Split(':')[1];
                        var primaryKey = monitorKey;
                        List<Dictionary<string, AttributeValue>> itemMasterList;

                        if (monitorKey == "ALL")
                        {
                            context.Logger.LogLine("Message for ALL found.  Deleting.");
                            await DeleteMessageOffQueue(context, message.ReceiptHandle, newMetricsTasksQueueUrl, sqsClient);
                            continue;
                        }

                        var stopWatch = new Stopwatch();
                        stopWatch.Start();
                        context.Logger.LogLine("Starting stopwatch");
                        using (var dynamoClient = new AmazonDynamoDBClient())
                        {
                            context.Logger.LogLine("Getting data from DynamoDB for monitor key " + monitorKey + " for " + dateToQuery);

                            var accountsTableName = Environment.GetEnvironmentVariable("Environment") + "-Accounts";

                            var request = new QueryRequest
                            {
                                TableName = accountsTableName,
                                KeyConditionExpression = "MONITOR_KEY = :monitor_key",
                                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                                {
                                    {
                                        ":monitor_key", new AttributeValue {S = primaryKey}
                                    }
                                }
                            };

                            context.Logger.LogLine("Getting items for the day");
                            var response = await dynamoClient.QueryAsync(request);

                            itemMasterList = response.Items;

                            while (response.LastEvaluatedKey.Count > 0)
                            {
                                context.Logger.LogLine("Getting more items for the day");
                                request = new QueryRequest
                                {
                                    TableName = accountsTableName,
                                    KeyConditionExpression = "MONITOR_KEY= :monitor_key",
                                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                                    {
                                        {
                                            ":monitor_key", new AttributeValue {S = primaryKey}
                                        }
                                    },
                                    ExclusiveStartKey = response.LastEvaluatedKey
                                };

                                response = await dynamoClient.QueryAsync(request);

                                itemMasterList.AddRange(response.Items);
                            }
                            stopWatch.Stop();
                            context.Logger.LogLine("Time to query: " + stopWatch.ElapsedMilliseconds);
                        }

                        context.Logger.LogLine("Calculating metrics");
                        stopWatch.Restart();

                        //OVERALL METRICS
                        var accountList = itemMasterList.Select(item => new Account(item, context)).ToList();
                        var totalNumberOfAccounts = accountList.Count;
                        context.Logger.LogLine("totalNumberOfAccounts: " + totalNumberOfAccounts);
                        var numberOfActiveAccounts = accountList.Count(a => a.AccountDisabledOrExpired == "0");
                        var numberOfImplementers = accountList.Count(a => a.IsImplementer == "1");
                        var numberOfIntegrationUsers = accountList.Count(a => a.IsIntegrationUser == "1");
                        var numberOfNewAccounts = accountList.Count(a => a.Initiated.Date.ToString("MM-dd-yyyy", CultureInfo.InvariantCulture) == dateToQuery);

                        using (var dynamoClient = new AmazonDynamoDBClient())
                        {
                            var item = new Document
                            {
                                ["MONITOR_KEY_DAY"] = primaryKey + "_" + dateToQuery,

                                ["TOTAL_NUMBER_OF_ACCOUNTS"] = totalNumberOfAccounts,
                                ["NUMBER_OF_ACTIVE_ACCOUNTS"] = numberOfActiveAccounts,
                                ["NUMBER_OF_IMPLEMENTERS"] = numberOfImplementers,
                                ["NUMBER_OF_INTEGRATION_USERS"] = numberOfIntegrationUsers,
                                ["NUMBER_OF_NEW_ACCOUNTS"] = numberOfNewAccounts
                            };

                            context.Logger.LogLine("Putting daily metric into DynamoDB");
                            var dailyMetricsTableName = Environment.GetEnvironmentVariable("Environment") + "-AccountDailyMetrics";
                            var table = Table.LoadTable(dynamoClient, dailyMetricsTableName);

                            try
                            {
                                context.Logger.LogLine("Upserting item into Dynamo");
                                await table.PutItemAsync(item);

                                context.Logger.LogLine("Successfully upserted item into Dynamo.  Deleting message off queue");
                                await DeleteMessageOffQueue(context, message.ReceiptHandle, newMetricsTasksQueueUrl, sqsClient);
                            }
                            catch (Exception e)
                            {
                                context.Logger.LogLine(e.Message);
                                context.Logger.LogLine("Failed to put item in DynamoDB.  Leaving message on the queue.");
                            }
                        }

                        stopWatch.Stop();
                        context.Logger.LogLine("Time to calculate the metrics: " + stopWatch.ElapsedMilliseconds);
                    }
                }

                context.Logger.LogLine("Daily metrics written to DynamoDb table");

                return "Success";
            }
            catch (Exception e)
            {
                context.Logger.LogLine($"Error getting messages from SQS and querying/saving to DynamoDB");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }

        private static async Task DeleteMessageOffQueue(ILambdaContext context, string receiptHandle, string newEventsQueueUrl, AmazonSQSClient sqsClient)
        {
            var deleteMessageRequest = new DeleteMessageRequest
            {
                QueueUrl = newEventsQueueUrl,
                ReceiptHandle = receiptHandle
            };
            context.Logger.LogLine("Deleting message off queue");
            await sqsClient.DeleteMessageAsync(deleteMessageRequest);
        }

        public static DateTime StartOfWeek(DateTime dt)
        {
            var diff = dt.DayOfWeek - DayOfWeek.Sunday;
            if (diff < 0)
            {
                diff += 7;
            }
            return dt.AddDays(-1 * diff).Date;
        }
    }

}

