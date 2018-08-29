using DotNet.Extensions.FileProviders;
using Microsoft.Diagnostics.EventFlow;
using Microsoft.Diagnostics.EventFlow.Inputs;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.IO;

namespace PlayGround
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("PlayGround.exe <projectId> <datasetId>");
                return;
            }
            var projectId = args[0];
            var datasetId = args[1];

            var eventFlowConfigJson = File.ReadAllText("eventFlowConfig.json");
            eventFlowConfigJson = eventFlowConfigJson
                .Replace("<projectId>", projectId)
                .Replace("<datasetId>", datasetId);
            var fileProvider = new StringFileProvider(eventFlowConfigJson);
            var config = new ConfigurationBuilder().AddJsonFile(fileProvider, "eventFlowConfig.json", false, false).Build();
            using (var pipeline = DiagnosticPipelineFactory.CreatePipeline(config))
            {
                PlayWithTrace(pipeline);
                PlayWithEventSource(pipeline);
                PlayWithMicrosoftLogging(pipeline);

                Console.WriteLine("Press any key to exit...");
                Console.ReadKey(intercept: true);
            }
        }

        static void PlayWithTrace(DiagnosticPipeline pipeline)
        {
            Trace.TraceWarning("EventFlow is working!");
        }

        static void PlayWithEventSource(DiagnosticPipeline pipeline)
        {
            PlayGroundEventSource.Log.Trace("hello PlayWithEventSource", 123, Environment.MachineName);

            try
            {
                throw new Exception("Boom");
            }
            catch (Exception ex)
            {
                PlayGroundEventSource.Log.Trace($"Error occurred. ex = {ex.ToString()}", 123, Environment.MachineName);
            }
        }

        static void PlayWithMicrosoftLogging(DiagnosticPipeline pipeline)
        {
            var loggerFactory = new LoggerFactory()
                .AddEventFlow(pipeline);

            var logger = new Logger<Program>(loggerFactory);
            using (logger.BeginScope("UserId:{UserId},MachineName:{MachineName}",
                123, Environment.MachineName))
            {
                logger.LogInformation("Hello from {friend}!", "EventFlow");
                var loggerMyTask = new Logger<MyTask>(loggerFactory);
                var my = new MyTask(loggerMyTask);
                my.Do("Task01");
            }
        }
    }

    class MyTask
    {
        private ILogger logger { get; set; }

        public MyTask(ILogger<MyTask> logger)
        {
            this.logger = logger;
        }

        public void Do(string taskName)
        {
            using (logger.BeginScope("TaskName:{TaskName}", taskName))
            {
                logger.LogInformation("Hi! this is {Name}", "MyTask");

                try
                {
                    throw new Exception("Boom!");
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Error occurred");
                }
            }
        }
    }
}
