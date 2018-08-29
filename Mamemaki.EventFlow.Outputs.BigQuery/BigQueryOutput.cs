using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Configuration;
using Validation;
using Microsoft.Diagnostics.EventFlow.Utilities;
using Microsoft.Diagnostics.EventFlow;
using Mamemaki.EventFlow.Outputs.BigQuery.Configuration;
using Newtonsoft.Json;
using Google.Apis.Util;
using Google.Apis.Bigquery.v2;
using Google.Apis.Bigquery.v2.Data;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google;

namespace Mamemaki.EventFlow.Outputs.BigQuery
{
    public class BigQueryOutput : IOutput, IDisposable
    {
        private readonly IHealthReporter healthReporter;
        private BigQueryOutputConfiguration Config { get; set; }
        private BigqueryService _BQSvc;
        private IBackOff _BackOff;

        public TableSchema TableSchema { get; private set; }

        /// <summary>
        /// Specified field value use for InsertId(<see cref="https://cloud.google.com/bigquery/streaming-data-into-bigquery#dataavailability"/>).
        /// If set "%uuid%" then generate uuid each time.
        /// </summary>
        public string InsertIdFieldName { get; private set; }

        public string TableIdExpanded
        {
            get
            {
                return _TableIdExpanded;
            }
            set
            {
                if (String.Compare(_TableIdExpanded, value) != 0)
                {
                    _TableIdExpanded = value;
                    _NeedCheckTableExists = true;
                }
            }
        }
        private string _TableIdExpanded;
        private bool _TableIdExpandable;
        private bool _NeedCheckTableExists;

        public BigQueryOutput(IConfiguration configuration, IHealthReporter healthReporter)
        {
            Requires.NotNull(configuration, nameof(configuration));
            Requires.NotNull(healthReporter, nameof(healthReporter));

            this.healthReporter = healthReporter;
            var bqOutputConfiguration = new BigQueryOutputConfiguration();
            try
            {
                configuration.Bind(bqOutputConfiguration);
            }
            catch
            {
                healthReporter.ReportProblem($"Invalid {nameof(BigQueryOutput)} configuration encountered: '{configuration.ToString()}'",
                    EventFlowContextIdentifiers.Configuration);
                throw;
            }

            Initialize(bqOutputConfiguration);
        }

        public BigQueryOutput(BigQueryOutputConfiguration bqOutputConfiguration, IHealthReporter healthReporter)
        {
            Requires.NotNull(bqOutputConfiguration, nameof(bqOutputConfiguration));
            Requires.NotNull(healthReporter, nameof(healthReporter));

            this.healthReporter = healthReporter;
            Initialize(bqOutputConfiguration);
        }

        private void Initialize(BigQueryOutputConfiguration bqOutputConfiguration)
        {
            Debug.Assert(bqOutputConfiguration != null);
            Debug.Assert(this.healthReporter != null);

            if (String.IsNullOrEmpty(bqOutputConfiguration.ProjectId))
                throw new ArgumentException("ProjectId");
            if (String.IsNullOrEmpty(bqOutputConfiguration.DatasetId))
                throw new ArgumentException("DatasetId");
            if (String.IsNullOrEmpty(bqOutputConfiguration.TableId))
                throw new ArgumentException("TableId");

            this.Config = bqOutputConfiguration;

            // Load table schema file
            if (Config.TableSchemaFile != null)
                TableSchema = LoadTableSchema(Config.TableSchemaFile);
            if (TableSchema == null)
                throw new Exception("table schema not set");
            // Expand table id 1st time within force mode
            ExpandTableIdIfNecessary(force: true);
            // configure finished
            healthReporter.ReportHealthy("TableId: " + TableIdExpanded);

            var scopes = new[]
            {
                BigqueryService.Scope.Bigquery,
                BigqueryService.Scope.BigqueryInsertdata,
                BigqueryService.Scope.CloudPlatform,
                BigqueryService.Scope.DevstorageFullControl
            };
            var credential = GoogleCredential.GetApplicationDefault()
                .CreateScoped(scopes);

            _BQSvc = new BigqueryService(new BaseClientService.Initializer
            {
                HttpClientInitializer = credential,
            });
            _BackOff = new ExponentialBackOff();
        }

        public async Task SendEventsAsync(IReadOnlyCollection<EventData> events, long transmissionSequenceNumber, 
            CancellationToken cancellationToken)
        {
            if (events == null || events.Count == 0)
            {
                return;
            }

            try
            {
                var rows = events.Select(s => CreateRowData(s)).ToList();
                await InsertDataAsync(rows, cancellationToken);
                this.healthReporter.ReportHealthy();
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception ex)
            {
                ErrorHandlingPolicies.HandleOutputTaskError(ex, () =>
                {
                    string errorMessage = nameof(BigQueryOutput) + ": Failed to write events to Bq." + Environment.NewLine + ex.ToString();
                    this.healthReporter.ReportWarning(errorMessage, EventFlowContextIdentifiers.Output);
                });
            }
        }

        public void Dispose()
        {
            if (_BQSvc != null)
            {
                _BQSvc.Dispose();
                _BQSvc = null;
            }
        }

        public async Task InsertDataAsync(IList<TableDataInsertAllRequest.RowsData> rows,
            CancellationToken cancellationToken)
        {
            ExpandTableIdIfNecessary();
            await EnsureTableExistsAsync(cancellationToken);

            var req = new TableDataInsertAllRequest
            {
                Rows = rows
            };
            var rowsCount = req.Rows.Count;
            var retry = 1;
            while (retry < _BackOff.MaxNumOfRetries)
            {
                try
                {
                    var response = await _BQSvc.Tabledata.InsertAll(req,
                            Config.ProjectId, Config.DatasetId, TableIdExpanded)
                            .ExecuteAsync(cancellationToken);
                    if (response.InsertErrors == null || !response.InsertErrors.Any())
                    {
                        return;
                    }

                    var messages = response.InsertErrors
                        .Zip(req.Rows, (x, r) => x.Errors.Select(e => new { x, r, e }).ToArray())
                        .SelectMany(xs => xs)
                        .Where(x => x.e.Reason != "stopped")
                        .Select(x =>
                        {
                            return string.Format(@"Index:{0}
DebugInfo:{1}
ETag:{2}
Location:{3}
Message:{4}
Reason:{5}
PostRawJSON:{6}",
                                x.x.Index, x.e.DebugInfo, x.e.ETag, x.e.Location,
                                x.e.Message, x.e.Reason,
                                JsonConvert.SerializeObject(x.r.Json, Formatting.None));
                        });
                    this.healthReporter.ReportWarning(String.Join("\n", messages), EventFlowContextIdentifiers.Output);
                }
                catch (OperationCanceledException)
                {
                    throw;
                }
                catch (GoogleApiException ex)
                {
                    string errorMessage = nameof(BigQueryOutput) + ": insert has failed." + Environment.NewLine + ex.ToString();
                    this.healthReporter.ReportWarning(errorMessage, EventFlowContextIdentifiers.Output);

                    if (ex.HttpStatusCode == System.Net.HttpStatusCode.Unauthorized)
                    {
                        return; // something wrong in authentication. no retry
                    }
                }
                catch (Exception ex)
                {
                    string errorMessage = nameof(BigQueryOutput) + ": insert has failed." + Environment.NewLine + ex.ToString();
                    this.healthReporter.ReportWarning(errorMessage, EventFlowContextIdentifiers.Output);
                    return;
                }

                retry++;
                await Task.Delay(_BackOff.GetNextBackOff(retry));
            }

            this.healthReporter.ReportWarning(nameof(BigQueryOutput) + ": Retry over.", EventFlowContextIdentifiers.Output);
        }

        async Task EnsureTableExistsAsync(CancellationToken cancellationToken)
        {
            if (!Config.AutoCreateTable)
                return;
            if (_NeedCheckTableExists)
            {
                if (!await IsTableExistsAsync(cancellationToken))
                {
                    await CreateTableAsync(cancellationToken);
                }
                _NeedCheckTableExists = false;
            }
        }

        async Task<bool> IsTableExistsAsync(CancellationToken cancellationToken)
        {
            try
            {
                var table = await _BQSvc.Tables.Get(Config.ProjectId, Config.DatasetId, TableIdExpanded)
                    .ExecuteAsync(cancellationToken);
                return true;
            }
            catch (GoogleApiException ex)
            {
                if (ex.HttpStatusCode == System.Net.HttpStatusCode.NotFound &&
                    ex.Message.Contains("Not found: Table"))
                {
                    return false;
                }
                throw;
            }
            catch (Exception)
            {
                throw;
            }
        }

        async Task CreateTableAsync(CancellationToken cancellationToken)
        {
            try
            {
                var table = new Table
                {
                    TableReference = new TableReference
                    {
                        ProjectId = Config.ProjectId,
                        DatasetId = Config.DatasetId,
                        TableId = TableIdExpanded
                    },
                    Schema = TableSchema,
                };

                await _BQSvc.Tables.Insert(table, Config.ProjectId, Config.DatasetId)
                    .ExecuteAsync(cancellationToken);
                this.healthReporter.ReportHealthy(nameof(BigQueryOutput) + $": Table({TableIdExpanded}) created.",
                    EventFlowContextIdentifiers.Output);
            }
            catch (OperationCanceledException)
            {
                throw;
            }
            catch (GoogleApiException ex)
            {
                if (ex.HttpStatusCode == System.Net.HttpStatusCode.Conflict &&
                    ex.Message.Contains("Already Exists: Table"))
                {
                    return;
                }

                this.healthReporter.ReportProblem(nameof(BigQueryOutput) + ": Failed to create table." + Environment.NewLine + ex.ToString(), 
                    EventFlowContextIdentifiers.Output);
            }
            catch (Exception ex)
            {
                this.healthReporter.ReportProblem(nameof(BigQueryOutput) + ": Failed to create table." + Environment.NewLine + ex.ToString(),
                    EventFlowContextIdentifiers.Output);
                throw;
            }
        }

        TableSchema LoadTableSchema(string schemaFile)
        {
            try
            {
                var strJson = File.ReadAllText(schemaFile);
                var schema = new TableSchema();
                schema.Fields = JsonConvert.DeserializeObject<List<TableFieldSchema>>(strJson);
                return schema;
            }
            catch (Exception ex)
            {
                this.healthReporter.ReportProblem(nameof(BigQueryOutput) + ": Failed to load schema." + Environment.NewLine + ex.ToString(),
                    EventFlowContextIdentifiers.Output);
                throw;
            }
        }

        void ExpandTableIdIfNecessary(bool force = false)
        {
            if (force || _TableIdExpandable)
            {
                this.TableIdExpanded = Regex.Replace(Config.TableId, @"(\{.+})", delegate (Match m) {
                    var pattern = m.Value.Substring(1, m.Value.Length - 2);
                    _TableIdExpandable = true;
                    return DateTime.UtcNow.ToString(pattern);
                });
            }
        }

        public TableDataInsertAllRequest.RowsData CreateRowData(EventData eventEntry)
        {
            var properties = new Dictionary<string, object>();
            foreach (var fieldSchema in TableSchema.Fields)
            {
                var val = GetValueByFieldName(eventEntry, fieldSchema.Name);
                if (val == null)
                {
                    if (fieldSchema.Mode == "REQUIRED")
                    {
                        throw new Exception($"No value for the field({fieldSchema.Name})");
                    }
                }
                else
                {
                    properties[fieldSchema.Name] = val;
                }
            }

            string insertId = null;
            if (InsertIdFieldName != null)
            {
                if (InsertIdFieldName == "%uuid%")
                {
                    insertId = Guid.NewGuid().ToString();
                }
                else
                {
                    insertId = GetValueByFieldName(eventEntry, InsertIdFieldName).ToString();
                    if (insertId == null)
                        throw new Exception($"No value for the field({InsertIdFieldName})");
                }
            }

            return new TableDataInsertAllRequest.RowsData
            {
                InsertId = insertId,
                Json = properties
            };
        }

        /// <summary>
        /// Get value corresponds to BigQuery field
        /// </summary>
        /// <param name="e"></param>
        /// <param name="fieldName"></param>
        /// <returns>string, int, DateTime. null if not found</returns>
        private object GetValueByFieldName(EventData e, string fieldName)
        {
            if (e.TryGetPropertyValue(fieldName, out object objVal))
            {
                return objVal;
            }
            return null;
        }
    }
}
