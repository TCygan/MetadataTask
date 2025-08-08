using FivetranClient;
using FivetranClient.Models;
using Import.Helpers.Fivetran;
using System.Text;

namespace Import.ConnectionSupport;

// equivalent of database is group in Fivetran terminology
public class FivetranConnectionSupport : IConnectionSupport
{
    public const string ConnectorTypeCode = "FIVETRAN";
    private record FivetranConnectionDetailsForSelection(string ApiKey, string ApiSecret);

    public object? GetConnectionDetailsForSelection()
    {
        Console.Write("Provide your Fivetran API Key: ");
        var apiKey = Console.ReadLine() ?? throw new ArgumentNullException();
        Console.Write("Provide your Fivetran API Secret: ");
        var apiSecret = ReadSecretFromConsole() ?? throw new ArgumentNullException();

        return new FivetranConnectionDetailsForSelection(apiKey, apiSecret);
    }

    private string ReadSecretFromConsole()
    {
        var secret = new StringBuilder();

        while (true)
        {
            var key = Console.ReadKey(intercept: true);
            if (key.Key == ConsoleKey.Enter)
            {
                Console.WriteLine();
                break;
            }

            if (key.Key == ConsoleKey.Backspace && secret.Length > 0)
            {
                secret.Length--;
                Console.Write("\b \b"); // handle backspace
            }
            else if (!char.IsControl(key.KeyChar))
            {
                secret.Append(key.KeyChar);
                Console.Write("*"); // mask input
            }
        }

        return secret.ToString();
    }

    public object GetConnection(object? connectionDetails, string? selectedToImport)
    {
        if (connectionDetails is not FivetranConnectionDetailsForSelection details)
        {
            throw new ArgumentException("Invalid connection details provided.");
        }

        return new RestApiManagerWrapper(
            new RestApiManager(
                details.ApiKey,
                details.ApiSecret,
                TimeSpan.FromSeconds(40)),
            selectedToImport ?? throw new ArgumentNullException(nameof(selectedToImport)));
    }

    //if we only want to call Dispose it looks cleaner
    public void CloseConnection(object? connection)
    {
        if (connection is IDisposable disposable)
            disposable.Dispose();
        else
            throw new ArgumentException("Invalid connection type provided.");
    }

    public string? SelectToImport(object? connectionDetails)
    {
        if (connectionDetails is not FivetranConnectionDetailsForSelection details)
            throw new ArgumentException("Invalid connection details provided.");

        var cts = new CancellationTokenSource();
        cts.CancelAfter(30_000);
        using var restApiManager = new RestApiManager(details.ApiKey, details.ApiSecret, TimeSpan.FromSeconds(40));
        var groups = restApiManager
            .GetGroupsAsync(cts.Token)
            .ToBlockingEnumerable()
            .ToArray();

        if (groups == null || groups.Length == 0)
            throw new Exception("No groups found in Fivetran account.");

        PrintGroups(groups);

        return TrySelectGroup(groups);
    }

    private void PrintGroups(Group[] groups)
    {
        var sb = new StringBuilder();
        sb.AppendLine("Available groups in Fivetran account:");

        for (int i = 0; i < groups.Length; i++)
        {
            sb.Append(i + 1)
              .Append(". ")
              .Append(groups[i].Name)
              .Append(" (ID: ")
              .Append(groups[i].Id)
              .AppendLine(")");
        }

        sb.Append("Please select a group to import from (by number): ");
        Console.Write(sb.ToString());
    }

    private string? TrySelectGroup(Group[] groups)
    {
        var input = Console.ReadLine();
        if (string.IsNullOrWhiteSpace(input)
            || !int.TryParse(input, out var selectedIndex)
            || selectedIndex < 1
            || selectedIndex > groups.Length)
        {
            throw new ArgumentException("Invalid group selection.");
        }

        return groups[selectedIndex - 1].Id;
    }

    public void RunImport(object? connection)
    {
        if (connection is not RestApiManagerWrapper restApiManagerWrapper)
            throw new ArgumentException("Invalid connection type provided.");

        RunImportInternalAsync(restApiManagerWrapper).Wait();
    }

    private async Task RunImportInternalAsync(RestApiManagerWrapper restApiManagerWrapper)
    {
        var cts = new CancellationTokenSource();
        List<Task> tasks = new List<Task>();
        var _lock = new object();
        var allMappingsBuffer = new StringBuilder("Lineage mappings:\n");
        //it would be the best to read timeouts from config(timouet for whole import and timout for each api call type),
        //here I just use 40s for whole import but with larger imports it would be probably not enough
        cts.CancelAfter(40_000);

        var connectors = restApiManagerWrapper.RestApiManager
            .GetConnectorsAsync(restApiManagerWrapper.GroupId, cts.Token);

        await foreach (var connector in connectors)
        {
            //We could add SemaphorSlim to limit number of parallel processing if we would had cpu throtling
            tasks.Add(ProcessConnectorAsync(restApiManagerWrapper, _lock, allMappingsBuffer, connector, cts.Token));
        }

        if (tasks.Count == 0)
            throw new Exception("No connectors found in the selected group.");

        await Task.WhenAll(tasks);
        Console.WriteLine(allMappingsBuffer);
    }

    private async Task ProcessConnectorAsync(RestApiManagerWrapper restApiManagerWrapper, object _lock, StringBuilder allMappingsBuffer, Connector connector, CancellationToken token)
    {
        try
        {
            var connectorSchemas = await restApiManagerWrapper.RestApiManager
                .GetConnectorSchemasAsync(connector.Id, token);

            foreach (var schema in connectorSchemas?.Schemas ?? [])
            {
                foreach (var table in schema.Value?.Tables ?? [])
                {
                    var sb = new StringBuilder();
                    sb.Append("  ")
                    .Append(connector.Id)
                    .Append(": ")
                    .Append(schema.Key)
                    .Append(".")
                    .Append(table.Key)
                    .Append(" -> ")
                    .Append(schema.Value?.NameInDestination)
                    .Append(".")
                    .Append(table.Value.NameInDestination)
                    .Append('\n');
                    lock (_lock)
                    {
                        allMappingsBuffer.Append(sb);
                    }
                }
            }
        }
        catch (AggregateException ex)
        {
            var sb = new StringBuilder();
            sb.Append("  ").Append(connector.Id).Append(": Aggregated exception:\n");

            foreach (var inner in ex.Flatten().InnerExceptions)
            {
                sb.Append("    ")
                  .Append(inner.GetType().Name)
                  .Append(" - ")
                  .Append(inner.Message)
                  .Append('\n');
            }

            lock (_lock)
            {
                allMappingsBuffer.Append(sb);
            }
        }
        catch (OperationCanceledException ex)
        {
            var sb = new StringBuilder();
            sb.Append("  ")
            .Append(connector.Id)
            .Append(": Timeout")
            .Append('\n');

            lock (_lock)
            {
                allMappingsBuffer.Append(sb);
            }
        }
        catch (Exception ex)
        {
            var sb = new StringBuilder();
            sb.Append("  ")
            .Append(connector.Id)
            .Append(": Exception")
            .Append(ex.GetType().Name)
            .Append(" - ")
            .Append(ex.Message)
            .Append('\n');

            lock (_lock)
            {
                allMappingsBuffer.Append(sb);
            }
        }
    }
}