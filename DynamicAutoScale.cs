using System;
using System.Collections.Generic;
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

namespace AutoScaleDynamo
{
    public class Function
    {
        public async Task<string> FunctionHandler(ILambdaContext context)
        {
            var tableCapacities = GetDynamoTableCapacityModels(context);

            foreach (var tableCapacity in tableCapacities)
            {
                var numberOfMessagesInReadQueue = await GetNumberOfMessagesInQueue(context, tableCapacity.ReadQueueUrl);
                var numberOfMessagesInWriteQueue = await GetNumberOfMessagesInQueue(context, tableCapacity.WriteQueueUrl);

                var infoRequest = new DescribeTableRequest
                {
                    TableName = tableCapacity.TableName
                };

                await GetCurrentTableCapacity(context, infoRequest, tableCapacity);

                var needsUpdate = false;
                int newWriteCapacity;
                int newReadCapacity;
                if (numberOfMessagesInReadQueue > tableCapacity.ReadQueueThreshold && tableCapacity.CurrentReadCapacity < tableCapacity.ElevatedReadCapacity)
                {
                    context.Logger.LogLine("Need to increase read capacity for " + tableCapacity.TableName);
                    needsUpdate = true;
                    newReadCapacity = tableCapacity.ElevatedReadCapacity;
                }
                else if (numberOfMessagesInReadQueue < tableCapacity.ReadQueueThreshold && tableCapacity.CurrentReadCapacity >= tableCapacity.ElevatedReadCapacity)
                {
                    context.Logger.LogLine("Need to decrease read capacity for " + tableCapacity.TableName);
                    needsUpdate = true;
                    newReadCapacity = tableCapacity.SteadyStateReadCapacity;
                }
                else
                {
                    context.Logger.LogLine("No read capacity change needed");
                    newReadCapacity = Convert.ToInt32(tableCapacity.CurrentReadCapacity);
                }

                if (numberOfMessagesInWriteQueue > tableCapacity.WriteQueueThreshold && tableCapacity.CurrentWriteCapacity < tableCapacity.ElevatedWriteCapacity)
                {
                    context.Logger.LogLine("Need to increase write capacity for " + tableCapacity.TableName);
                    needsUpdate = true;
                    newWriteCapacity = tableCapacity.ElevatedWriteCapacity;
                }
                else if (numberOfMessagesInWriteQueue < tableCapacity.WriteQueueThreshold && tableCapacity.CurrentWriteCapacity >= tableCapacity.ElevatedWriteCapacity)
                {
                    context.Logger.LogLine("Need to decrease write capacity for " + tableCapacity.TableName);
                    needsUpdate = true;
                    newWriteCapacity = tableCapacity.SteadyStateWriteCapacity;
                }
                else
                {
                    context.Logger.LogLine("No write capacity change needed");
                    newWriteCapacity = Convert.ToInt32(tableCapacity.CurrentWriteCapacity);
                }

                if (needsUpdate)
                {
                    await UpdateCapacity(context, tableCapacity.TableName, newReadCapacity, newWriteCapacity);
                }
            }

            context.Logger.LogLine("Done updating all table capacities");

            return "Success";
        }

        private static List<DynamoTableCapacity> GetDynamoTableCapacityModels(ILambdaContext context)
        {
            context.Logger.LogLine("Creating DynamoTableCapacities");
            var businessProcessEventsTableName = Environment.GetEnvironmentVariable("Environment") + "-BusinessProcessEvents";
            var businessProcessEventsQueueUrl = Environment.GetEnvironmentVariable("NewBusinessProcessEventsQueueUrl");
            var businessProcessMetricsTasksQueueUrl = Environment.GetEnvironmentVariable("BusinessProcessMetricsTasksQueueUrl");

            var businessProcessTypesTableName = Environment.GetEnvironmentVariable("Environment") + "-BusinessProcessTypes";

            var integrationEventsTableName = Environment.GetEnvironmentVariable("Environment") + "-IntegrationEvents";
            var integrationEventsQueueUrl = Environment.GetEnvironmentVariable("NewIntegrationEventsQueueUrl");
            var integrationMetricsTasksQueueUrl = Environment.GetEnvironmentVariable("IntegrationMetricsTasksQueueUrl");

            var loginEventsTableName = Environment.GetEnvironmentVariable("Environment") + "-LoginEvents";
            var loginEventsQueueUrl = Environment.GetEnvironmentVariable("NewLoginEventsQueueUrl");
            var loginMetricsTasksQueueUrl = Environment.GetEnvironmentVariable("LoginMetricsTasksQueueUrl");

            var accountsTableName = Environment.GetEnvironmentVariable("Environment") + "-Accounts";
            var accountsQueueUrl = Environment.GetEnvironmentVariable("NewAccountsQueueUrl");
            var accountMetricsTasksQueueUrl = Environment.GetEnvironmentVariable("AccountMetricsTasksQueueUrl");

            var bpPolicyChangeEventsTableName = Environment.GetEnvironmentVariable("Environment") + "-BPPolicyChangeEvents";
            var bpPolicyChangeEventsQueueUrl = Environment.GetEnvironmentVariable("NewBPPolicyChangeEventsQueueUrl");
            var bpPolicyChangeMetricsTasksQueueUrl = Environment.GetEnvironmentVariable("BPPolicyChangeMetricsTasksQueueUrl");

            var domainPolicyChangeTableName = Environment.GetEnvironmentVariable("Environment") + "-DomainPolicyChangeEvents";
            var domainPolicyChangeEventsQueueUrl = Environment.GetEnvironmentVariable("NewDomainPolicyChangeEventsQueueUrl");
            var domainPolicyChangeMetricsTasksQueueUrl = Environment.GetEnvironmentVariable("DomainPolicyChangeMetricsTasksQueueUrl");

            var securityGroupsTableName = Environment.GetEnvironmentVariable("Environment") + "-SecurityGroups";
            var securityGroupsQueueUrl = Environment.GetEnvironmentVariable("NewSecurityGroupsQueueUrl");
            var securityGroupsMetricsTasksQueueUrl = Environment.GetEnvironmentVariable("SecurityGroupsMetricsTasksQueueUrl");

            context.Logger.LogLine("Environmental variables retrieved");
            var tableCapacities = new List<DynamoTableCapacity>
            {
                new DynamoTableCapacity
                {
                    TableName = businessProcessEventsTableName,
                    SteadyStateWriteCapacity = 10,
                    ElevatedWriteCapacity = 300,
                    SteadyStateReadCapacity = 10,
                    ElevatedReadCapacity = 300,
                    WriteQueueUrl = businessProcessEventsQueueUrl,
                    WriteQueueThreshold = 1000,
                    ReadQueueUrl = businessProcessMetricsTasksQueueUrl,
                    ReadQueueThreshold = 1
                },
                new DynamoTableCapacity
                {
                    TableName = businessProcessTypesTableName,
                    SteadyStateWriteCapacity = 10,
                    ElevatedWriteCapacity = 300,
                    SteadyStateReadCapacity = 10,
                    ElevatedReadCapacity = 300,
                    WriteQueueUrl = businessProcessEventsQueueUrl,
                    WriteQueueThreshold = 1000,
                    ReadQueueUrl = businessProcessMetricsTasksQueueUrl,
                    ReadQueueThreshold = 1
                },
                new DynamoTableCapacity
                {
                    TableName = integrationEventsTableName,
                    SteadyStateWriteCapacity = 10,
                    ElevatedWriteCapacity = 300,
                    SteadyStateReadCapacity = 10,
                    ElevatedReadCapacity = 300,
                    WriteQueueUrl = integrationEventsQueueUrl,
                    WriteQueueThreshold = 1000,
                    ReadQueueUrl = integrationMetricsTasksQueueUrl,
                    ReadQueueThreshold = 1
                },
                new DynamoTableCapacity
                {
                    TableName = loginEventsTableName,
                    SteadyStateWriteCapacity = 10,
                    ElevatedWriteCapacity = 400,
                    SteadyStateReadCapacity = 10,
                    ElevatedReadCapacity = 300,
                    WriteQueueUrl = loginEventsQueueUrl,
                    WriteQueueThreshold = 1000,
                    ReadQueueUrl = loginMetricsTasksQueueUrl,
                    ReadQueueThreshold = 1
                },
                new DynamoTableCapacity
                {
                    TableName = accountsTableName,
                    SteadyStateWriteCapacity = 10,
                    ElevatedWriteCapacity = 300,
                    SteadyStateReadCapacity = 10,
                    ElevatedReadCapacity = 500,
                    WriteQueueUrl = accountsQueueUrl,
                    WriteQueueThreshold = 1000,
                    ReadQueueUrl = accountMetricsTasksQueueUrl,
                    ReadQueueThreshold = 1
                },
                new DynamoTableCapacity
                {
                    TableName = bpPolicyChangeEventsTableName,
                    SteadyStateWriteCapacity = 10,
                    ElevatedWriteCapacity = 300,
                    SteadyStateReadCapacity = 10,
                    ElevatedReadCapacity = 300,
                    WriteQueueUrl = bpPolicyChangeEventsQueueUrl,
                    WriteQueueThreshold = 1000,
                    ReadQueueUrl = bpPolicyChangeMetricsTasksQueueUrl,
                    ReadQueueThreshold = 1
                },
                new DynamoTableCapacity
                {
                    TableName = domainPolicyChangeTableName,
                    SteadyStateWriteCapacity = 10,
                    ElevatedWriteCapacity = 300,
                    SteadyStateReadCapacity = 10,
                    ElevatedReadCapacity = 300,
                    WriteQueueUrl = domainPolicyChangeEventsQueueUrl,
                    WriteQueueThreshold = 1000,
                    ReadQueueUrl = domainPolicyChangeMetricsTasksQueueUrl,
                    ReadQueueThreshold = 1
                },
                new DynamoTableCapacity
                {
                    TableName = securityGroupsTableName,
                    SteadyStateWriteCapacity = 10,
                    ElevatedWriteCapacity = 300,
                    SteadyStateReadCapacity = 10,
                    ElevatedReadCapacity = 300,
                    WriteQueueUrl = securityGroupsQueueUrl,
                    WriteQueueThreshold = 1000,
                    ReadQueueUrl = securityGroupsMetricsTasksQueueUrl,
                    ReadQueueThreshold = 1
                }
            };
            return tableCapacities;
        }

        private static async Task<int> GetNumberOfMessagesInQueue(ILambdaContext context, string queueUrl)
        {
            var queueName = queueUrl.Split('/')[4];
            context.Logger.LogLine("Getting number of messages in queue: " + queueName);
            int approximateNumberOfMessagesInQueue;
            using (var sqsClient = new AmazonSQSClient())
            {
                var queueRequest = new GetQueueAttributesRequest
                {
                    QueueUrl = queueUrl,
                    AttributeNames = new List<string>
                    {
                        "ApproximateNumberOfMessages"
                    }
                };

                var queueResponse = await sqsClient.GetQueueAttributesAsync(queueRequest);
                approximateNumberOfMessagesInQueue = queueResponse.ApproximateNumberOfMessages;
                context.Logger.LogLine(string.Format("Approximate number of messages in {0} queue: {1}", queueName, approximateNumberOfMessagesInQueue));
            }
            return approximateNumberOfMessagesInQueue;
        }

        private static async Task GetCurrentTableCapacity(ILambdaContext context, DescribeTableRequest infoRequest, DynamoTableCapacity tableCapacity)
        {
            context.Logger.LogLine("Getting current capacity for table: " + tableCapacity.TableName);
            using (var dynamoClient = new AmazonDynamoDBClient())
            {
                var infoResponse = await dynamoClient.DescribeTableAsync(infoRequest);
                var description = infoResponse.Table;

                tableCapacity.CurrentReadCapacity = description.ProvisionedThroughput.ReadCapacityUnits;
                tableCapacity.CurrentWriteCapacity = description.ProvisionedThroughput.WriteCapacityUnits;

                context.Logger.LogLine("Provision Throughput (reads/sec): " + description.ProvisionedThroughput.ReadCapacityUnits);
                context.Logger.LogLine("Provision Throughput (writes/sec): " + description.ProvisionedThroughput.WriteCapacityUnits);
            }
        }

        private static async Task UpdateCapacity(ILambdaContext context, string tableName, int newReadCapacity, int newWriteCapacity)
        {
            context.Logger.LogLine("Updating capacity of table: " + tableName + ", newReadCapacity: " + newReadCapacity + ", newWriteCapacity: " + newWriteCapacity);

            UpdateTableRequest updateRequest;
            if (tableName.Contains("BusinessProcessEvents"))
            {
                updateRequest = new UpdateTableRequest
                {
                    TableName = tableName,
                    ProvisionedThroughput = new ProvisionedThroughput
                    {
                        ReadCapacityUnits = newReadCapacity,
                        WriteCapacityUnits = newWriteCapacity
                    },
                    GlobalSecondaryIndexUpdates = new List<GlobalSecondaryIndexUpdate>
                    {
                        new GlobalSecondaryIndexUpdate
                        {
                            Update = new UpdateGlobalSecondaryIndexAction
                            {
                                IndexName = "EVENT_DAY_BUSINESS_PROCESS_TYPE-index",
                                ProvisionedThroughput = new ProvisionedThroughput
                                {
                                    ReadCapacityUnits = Convert.ToInt32(1.5 * newReadCapacity),
                                    WriteCapacityUnits = Convert.ToInt32(1.5 * newWriteCapacity)
                                }
                            }
                        }
                    }
                };
            }
            else
            {

                updateRequest = new UpdateTableRequest
                {
                    TableName = tableName,
                    ProvisionedThroughput = new ProvisionedThroughput
                    {
                        ReadCapacityUnits = newReadCapacity,
                        WriteCapacityUnits = newWriteCapacity
                    }
                };
            }

            using (var dynamoClient = new AmazonDynamoDBClient())
            {
                var response = await dynamoClient.UpdateTableAsync(updateRequest);
            }

            context.Logger.LogLine("Done updating capacity of table: " + tableName);
        }

    }
}
