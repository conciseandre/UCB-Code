using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.Core;
using Amazon.SQS;
using Amazon.SQS.Model;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.

[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.Json.JsonSerializer))]

namespace LoadAccounts
{
    public class Function
    {
        public async Task<string> FunctionHandler(ILambdaContext context)
        {
            try
            {
                context.Logger.LogLine("Entering function");

                var newEventsQueueUrl = Environment.GetEnvironmentVariable("NewAccountsQueueUrl");

                var receiveMessageRequest = new ReceiveMessageRequest
                {
                    QueueUrl = newEventsQueueUrl,
                    MaxNumberOfMessages = 10
                };

                using (var sqsClient = new AmazonSQSClient())
                {
                    var pollForMoreMessages = true;
                    while (pollForMoreMessages && (context.RemainingTime > new TimeSpan(0, 0, 5)))
                    {
                        context.Logger.LogLine(string.Format("Looking for messages on the queue.  Remaining time: {0} min {1} sec", context.RemainingTime.Minutes, context.RemainingTime.Seconds));
                        var receiveMessageResponse = await sqsClient.ReceiveMessageAsync(receiveMessageRequest);
                        pollForMoreMessages = receiveMessageResponse.Messages.Count > 0;
                        context.Logger.LogLine(string.Format("Messages retrieved: {0}", receiveMessageResponse.Messages.Count));

                        using (var dynamoClient = new AmazonDynamoDBClient())
                        {
                            var tableName = Environment.GetEnvironmentVariable("Environment") + "-Accounts";
                            var table = Table.LoadTable(dynamoClient, tableName);
                            foreach (var message in receiveMessageResponse.Messages)
                            {
                                var item = BuildDynamoItem(context, message);

                                if (item.Count == 0)
                                {
                                    context.Logger.LogLine("Deleting corrupted message off the queue");
                                    await DeleteMessageOffQueue(context, message.ReceiptHandle, newEventsQueueUrl, sqsClient);
                                    continue;
                                }

                                try
                                {
                                    context.Logger.LogLine("Upserting item into Dynamo");
                                    await table.PutItemAsync(item);

                                    context.Logger.LogLine("Successfully upserted item into Dynamo.  Deleting message off queue");
                                    await DeleteMessageOffQueue(context, message.ReceiptHandle, newEventsQueueUrl, sqsClient);
                                }
                                catch (Exception e)
                                {
                                    context.Logger.LogLine(e.Message);
                                    context.Logger.LogLine("Failed to put item in DynamoDB.  Leaving message on the queue.");
                                }
                            }
                        }
                    }
                }

                context.Logger.LogLine("Accounts written to DynamoDb table");

                return "Success";
            }
            catch (Exception e)
            {
                context.Logger.LogLine($"Error getting messages from SQS and saving to DynamoDB");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
        }

        private static async Task DeleteMessageOffQueue(ILambdaContext context,
            string receiptHandle,
            string newEventsQueueUrl,
            AmazonSQSClient sqsClient)
        {
            var deleteMessageRequest = new DeleteMessageRequest
            {
                QueueUrl = newEventsQueueUrl,
                ReceiptHandle = receiptHandle
            };
            context.Logger.LogLine("Deleting message off queue");
            await sqsClient.DeleteMessageAsync(deleteMessageRequest);
        }

        private static Document BuildDynamoItem(ILambdaContext context, Message message)
        {
            var unsanitizedMessageParts = Regex.Split(message.Body, ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
            var messageParts = unsanitizedMessageParts.Select(x => x.Replace("\"", string.Empty)).ToArray();
            if (messageParts.Length != 14)
            {
                context.Logger.LogLine("INCORRECT PARSING");
                foreach (var messagePart in messageParts)
                {
                    context.Logger.LogLine(messagePart);
                }
                return new Document();
            }

            context.Logger.LogLine("Creating Item for DynamoDB");

            var item = new Document
            {
                ["MONITOR_KEY"] = messageParts[0],
                ["WORKDAY_ID"] = messageParts[1],
                ["INITIATED"] = messageParts[2],
                ["LAST_FUNCTIONALLY_UPDATED"] = messageParts[3],
                ["IS_IMPLEMENTER"] = messageParts[4],
                ["IS_INTEGRATION_USER"] = messageParts[5],
                ["ACCOUNT_DISABLED_OR_EXPIRED"] = messageParts[6],
                ["ACCOUNT_LOCKED_DISABLED_OR_EXPIRED"] = messageParts[7],
                ["ALLOW_MIXED_LANGUAGE_TRANSACTIONS"] = messageParts[8],
                ["COUNT_OF_INVALID_CREDENTIAL_ATTEMPTS_SINCE_LAST_SUCCESSFUL_SIGN_ON"] = int.Parse(messageParts[9]),
                ["COUNT_OF_UNSUCCESSFUL_SIGN_ON_ATTEMPTS_SINCE_LAST_SUCCESSFUL_SIGN_ON"] = int.Parse(messageParts[10]),
                ["CURRENTLY_LOCKED_INVALID_CREDENTIALS"] = messageParts[11],
                ["DAYS_SINCE_LAST_PASSWORD_CHANGE"] = int.Parse(messageParts[12]),
                ["DAYS_TO_SHOW_COMPLETED_TASKS"] = int.Parse(messageParts[13])
            };

            return item;
        }
    }
}
