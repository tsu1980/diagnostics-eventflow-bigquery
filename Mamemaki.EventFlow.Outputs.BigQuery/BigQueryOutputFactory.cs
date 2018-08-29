using Microsoft.Diagnostics.EventFlow;
using Microsoft.Extensions.Configuration;
using Validation;

namespace Mamemaki.EventFlow.Outputs.BigQuery
{
    public class BigQueryOutputFactory : IPipelineItemFactory<BigQueryOutput>
    {
        public BigQueryOutput CreateItem(IConfiguration configuration, IHealthReporter healthReporter)
        {
            Requires.NotNull(configuration, nameof(configuration));
            Requires.NotNull(healthReporter, nameof(healthReporter));

            return new BigQueryOutput(configuration, healthReporter);
        }
    }
}
