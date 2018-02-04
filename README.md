https://cloudcraft.co/view/7d0ab720-0112-4305-86d2-ff94d32df6c1?key=fvjJlWr2gbINMrxWDH0Bgg



The system is basically 7 applications, one for each of the event/file types, with some common shared components.  The seven event/file types are:

Accounts
BP Policy Change Events
Business Process Events
Domain Policy Change Events
Integration Events
Login Events
Security Groups
For each of these, there are three stages to the process:

Load the Data
Generate the Metrics
Generate the Report
Email the Report
Stage 1: Load the Data
There is an S3 bucket for each of those in which Workday drops the data export.  This is done on a daily basis.  The S3 bucket names for those data dumps are as follows:

prod-mercer-accounts
prod-mercer-bp-policy-change-events
prod-mercer-business-process-events
prod-mercer-domain-policy-change-events
prod-mercer-integration-events
prod-mercer-login-events
prod-mercer-security-groups
When a new file is dropped in any of those buckets, the BatchEvents Lambda function is triggered.  This Lambda function batches the files in smaller files and drops them into the -batched S3 buckets.  The S3 bucket names for those are as follows:

prod-mercer-accounts-batched
prod-mercer-bp-policy-change-events-batched
prod-mercer-business-process-events-batched
prod-mercer-domain-policy-change-events-batched
prod-mercer-integration-events-batched
prod-mercer-login-events-batched
prod-mercer-security-groups-batched
When a new file is dropped into one of those S3 buckets, the QueueEvents Lambda function is triggered.  This Lambda function reads each row of data from the batched files and puts messages on the various SQS queues (1 message/row of data).  The names of the SQS queues for these message are as follows:

prod-NewAccounts
prod-NewBPPolicyChangeEvents
prod-NewBusinessProcessEvents
prod-NewDomainPolicyChangeEvents
prod-NewIntegrationEvents
prod-NewLoginEvents
prod-NewSecurityGroups
From here, the Lambda function become more event/file specific until the very end of the process.  For each of those seven queues, there is a Lambda function reading message off the queue.  These seven Lambda functions are as follows:

prod-LoadAccounts
prod-LoadBPPolicyChangeEvents
prod-LoadBusinessProcessEvents
prod-LoadDomainPolicyChangeEvents
prod-LoadIntegrationEvents
prod-LoadLoginEvents
prod-LoadSecurityGroups

Each of those Lambda functions reads the message if its respective queue, validates it, and loads the item in DynamoDB.  The seven tables the data is loaded in are as follows:

prod-Accounts
prod-BPPolicyChangeEvents
prod-BusinessProcessEvents
prod-DomainPolicyChangeEvents
prod-IntegrationEvents
prod-LoginEvents
prod-SecurityGroups

At this point, the data is loaded into DynamoDB.  The next part of the process is to generate metrics off that data.

Stage 2: Generate the Metrics
Each night, the prod-QueueMetricsTasksForTenant Lambda function fires.  This Lambda function puts 1 message/tenant/queue on seven queues for daily metrics and seven queues for weekly metrics.  The seven daily metrics tasks queues are as follows:

prod-AccountDailyMetricsTasks
prod-BPPolicyChangeDailyMetricsTasks
prod-BusinessProcessDailyMetricsTasks
prod-DomainPolicyChangeDailyMetricsTasks
prod-IntegrationDailyMetricsTasks
prod-LoginDailyMetricsTasks
prod-SecurityGroupDailyMetricsTasks
And the seven weekly metrics tasks queues are as follows:

prod-AccountWeeklyMetricsTasks
prod-BPPolicyChangeWeeklyMetricsTasks
prod-BusinessProcessWeeklyMetricsTasks
prod-DomainPolicyChangeWeeklyMetricsTasks
prod-IntegrationWeeklyMetricsTasks
prod-LoginWeeklyMetricsTasks
prod-SecurityGroupWeeklyMetricsTasks

For each of those queues, there is a Lambda function reading the messages and generating the appropriate metrics.  The names of the seven Lambda functions that generate daily metrics are as follows:

prod-GenerateAccountDailyMetrics
prod-GenerateBPPolicyChangeDailyMetrics
prod-GenerateBusinessProcessDailyMetrics
prod-GenerateDomainPolicyChangeDailyMetrics
prod-GenerateIntegrationDailyMetrics
prod-GenerateLoginDailyMetrics
prod-GenerateSecurityGroupDailyMetrics 
These Lamba functions read individual events from the events tables, generate metrics, and then write the metrics to the following seven tables:

prod-AccountDailyMetrics
prod-BPPolicyChangeDailyMetrics
prod-BusinessProcessDailyMetrics
prod-DomainPolicyChangeDailyMetrics
prod-IntegrationDailyMetrics
prod-LoginDailyMetrics
prod-SecurityGroupDailyMetrics
The seven Lambdas that generate weekly metrics are as follows:

prod-GenerateAccountWeeklyMetrics
prod-GenerateBPPolicyChangeWeeklyMetrics
prod-GenerateBusinessProcessWeeklyMetrics
prod-GenerateDomainPolicyChangeWeeklyMetrics
prod-GenerateIntegrationWeeklyMetrics
prod-GenerateLoginWeeklyMetrics
prod-GenerateSecurityGroupWeeklyMetrics 

These Lambda functions read daily metrics from the daily metrics tables, generate metrics, and then write the metrics to the following seven tables:

prod-AccountWeeklyMetrics
prod-BPPolicyChangeWeeklyMetrics
prod-BusinessProcessWeeklyMetrics
prod-DomainPolicyChangeWeeklyMetrics
prod-IntegrationWeeklyMetrics
prod-LoginWeeklyMetrics
prod-SecurityGroupWeeklyMetrics
The completes Stage 2: Generate the Metrics

Stage 3: Generate the Report

At this point, the functionality across the various event/file types converges back together to form one report.  Each night, the prod-QueueReportForTenant Lambda function fires.  This Lambda function puts 1 message/tenant on the prod-ReportTasks queue.  

The prod-GenerateReportHtml Lambda function read messages off that queue.  For each message, it queries the seven weekly metrics tasks tables listed above, generates some additional metrics based off that data, generates the html of the report with the data filled in, and passes the fulll html to the GeneratePdfData Lambda function.

The prod-GeneratePdfData Lambda function takes the html and generates a PDF file based on it, then passes back the PDF file data.  

The prod-GenerateReportHtml Lambda function then takes that PDF file data and writes it to an actual function in the prod-mercer-client-reports S3 bucket.  This is the PDF file which an Admin can go download, is emailed out, etc.  

Stage 4: Email the Report

Each time a file is dropped in the prod-mercer-client-reports S3 bucket, the EmailReport Lambda function fires.  This Lambda function gets the email distribution list for that tenant from DynamoDB and emails the PDF as an attachment to those individuals.



That's it: from Workday data dump to final PDF report emailed out.


