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

namespace GenerateAccountWeeklyMetrics
{
    public class Function
    {
        public async Task<string> FunctionHandler(ILambdaContext context)
        {
            try
            {
                context.Logger.LogLine("Entering function");

                var newMetricsTasksQueueUrl = Environment.GetEnvironmentVariable("AccountWeeklyMetricsTasksQueueUrl");

                var receiveMessageRequest = new ReceiveMessageRequest
                {
                    QueueUrl = newMetricsTasksQueueUrl
                };

                using (var sqsClient = new AmazonSQSClient())
                {
                    var pollForMoreMessages = true;
                    while (pollForMoreMessages && context.RemainingTime > new TimeSpan(0, 0, 5))
                    {
                        context.Logger.LogLine(string.Format("Looking for messages on the queue.  Remaining time: {0} min {1} sec", context.RemainingTime.Minutes, context.RemainingTime.Seconds));
                        var receiveMessageResponse = await sqsClient.ReceiveMessageAsync(receiveMessageRequest);
                        pollForMoreMessages = receiveMessageResponse.Messages.Count > 0;
                        context.Logger.LogLine(string.Format("Messages retrieved: {0}", receiveMessageResponse.Messages.Count));

                        foreach (var message in receiveMessageResponse.Messages)
                        {
                            var messageParts = message.Body.Split(',');
                            var monitorKey = messageParts[0].Split(':')[1];
                            var weekStart = messageParts[1].Split(':')[1];
                            //var weekStart = "08-20-2017";
                            var itemMasterList = new List<Dictionary<string, AttributeValue>>();

                            if (monitorKey == "ALL")
                            {
                                context.Logger.LogLine("Message for ALL found.  Deleting message.");
                                await DeleteMessageOffQueue(context, message.ReceiptHandle, newMetricsTasksQueueUrl, sqsClient);
                                continue;
                            }

                            var stopWatch = new Stopwatch();
                            stopWatch.Start();
                            context.Logger.LogLine("Starting stopwatch");
                            using (var dynamoClient = new AmazonDynamoDBClient())
                            {
                                for (int i = 0; i < 7; i++)
                                {
                                    var dayToQuery = DateTime.Parse(weekStart).AddDays(i).Date.ToString("MM-dd-yyyy", CultureInfo.InvariantCulture);
                                    context.Logger.LogLine("Getting data from DynamoDB for monitor key " + monitorKey + " for " + dayToQuery);

                                    var accountDailyMetricsTableName = Environment.GetEnvironmentVariable("Environment") + "-AccountDailyMetrics";

                                    var primaryKey = monitorKey + "_" + dayToQuery;
                                    var request = new QueryRequest
                                    {
                                        TableName = accountDailyMetricsTableName,
                                        KeyConditionExpression = "MONITOR_KEY_DAY = :monitor_key_day",
                                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                                        {
                                            {
                                                ":monitor_key_day", new AttributeValue {S = primaryKey}
                                            }
                                        }
                                    };

                                    context.Logger.LogLine("Getting metrics for the day");
                                    var response = await dynamoClient.QueryAsync(request);

                                    itemMasterList.AddRange(response.Items);

                                    stopWatch.Stop();
                                    context.Logger.LogLine("Time to query: " + stopWatch.ElapsedMilliseconds);
                                }
                            }

                            context.Logger.LogLine("Calculating metrics");
                            stopWatch.Restart();

                            //OVERALL METRICS
                            var dailyMetricsList = itemMasterList.Select(item => new AccountDailyMetrics(item, context)).ToList();
                            
                            var totalNumberOfAccounts = dailyMetricsList.LastOrDefault().TotalNumberOfAccounts;
                            context.Logger.LogLine("totalNumberOfAccounts: " + totalNumberOfAccounts);

                            var numberOfActiveAccounts = dailyMetricsList.LastOrDefault().NumberOfActiveAccounts;
                            var numberOfImplementers = dailyMetricsList.LastOrDefault().NumberOfImplementers;
                            var numberOfIntegrationUsers = dailyMetricsList.LastOrDefault().NumberOfIntegrationUsers;
                            var numberOfNewAccounts = dailyMetricsList.Sum(dm => dm.NumberOfNewAccounts);
                            context.Logger.LogLine("numberOfNewAccounts for the week: " + numberOfNewAccounts);

                            context.Logger.LogLine("Finished calculating");

                            using (var dynamoClient = new AmazonDynamoDBClient())
                            {
                                context.Logger.LogLine("Preparing item to put into Dynamo");
                                var item = new Document
                                {
                                    ["MONITOR_KEY_WEEK"] = monitorKey + "_" + weekStart,

                                    ["TOTAL_NUMBER_OF_ACCOUNTS"] = totalNumberOfAccounts,
                                    ["NUMBER_OF_ACTIVE_ACCOUNTS"] = numberOfActiveAccounts,
                                    ["NUMBER_OF_IMPLEMENTERS"] = numberOfImplementers,
                                    ["NUMBER_OF_INTEGRATION_USERS"] = numberOfIntegrationUsers,
                                    ["NUMBER_OF_NEW_ACCOUNTS"] = numberOfNewAccounts
                                };
                                context.Logger.LogLine("Putting weekly metric into Dynamo for" + monitorKey + "_" + weekStart);
                                var accountWeeklyMetricsTableName = Environment.GetEnvironmentVariable("Environment") + "-AccountWeeklyMetrics";
                                var table = Table.LoadTable(dynamoClient, accountWeeklyMetricsTableName);
                                var putItemResponse = await table.PutItemAsync(item);

                                context.Logger.LogLine("Item saved to Dynamo");

                                var receiptHandle = message.ReceiptHandle;
                                var deleteMessageRequest = new DeleteMessageRequest
                                {
                                    QueueUrl = newMetricsTasksQueueUrl,
                                    ReceiptHandle = receiptHandle
                                };
                                context.Logger.LogLine("Deleting message off queue");
                                var deleteMessageResponse = await sqsClient.DeleteMessageAsync(deleteMessageRequest);

                            }

                            stopWatch.Stop();
                            context.Logger.LogLine("Time to calculate the metrics: " + stopWatch.ElapsedMilliseconds);
                        }
                    }
                }

                context.Logger.LogLine("Weekly metrics written to DynamoDb table");

                return "Success";
            }
            catch (Exception e)
            {
                context.Logger.LogLine($"Error generating weekly account metrics");
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
    }
}
