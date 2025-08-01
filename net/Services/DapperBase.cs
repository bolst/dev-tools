using Dapper;
using Npgsql;
using System.Text.Json;

// namespace ...;


public abstract class DapperBase
{

    private const int BATCH_SIZE = 200;

    protected readonly string _connectionString;

    private NpgsqlTransaction? transaction;
    private NpgsqlTransaction? _transaction
    {
        get
        {
            if (transaction is null) return transaction;

            try
            {
                // error will throw here if transaction is disposed
                var _ = transaction?.Connection;
            }
            catch
            {
                transaction = null;
            }

            return transaction;
        }
        set
        {
            transaction = value;
        }
    }

    public DapperBase(string connectionString)
    {
        _connectionString = connectionString;
    }




    /// <summary>
    /// Connects and queries the database using the set connection string.
    /// </summary>
    /// <typeparam name="T">The type of data being queried.</typeparam>
    /// <param name="query">The SQL query to be performed.</param>
    /// <param name="param">Parameters to use in <paramref name="query" />.</param>
    /// <param name="cancellationToken">The cancellation token for this command.</param>
    /// <returns>A sequence of data of <typeparamref name="T"/>; if a basic type (int, string, etc) is queried then the data from the first column is assumed, otherwise an instance is created per row, and a direct column-name===member-name mapping is assumed (case-insensitive). </returns>
    protected async Task<IEnumerable<T>> QueryDbAsync<T>(string query, object? param = null, CancellationToken cancellationToken = default)
    {
        var command = new CommandDefinition(query, param, transaction: transaction, cancellationToken: cancellationToken);
        return await QueryDbAsync<T>(command);
    }




    /// <summary>
    /// Connects and queries the database using the set connection string.
    /// </summary>
    /// <typeparam name="T">The type of data being queried.</typeparam>
    /// <param name="command">The command to perform.</param>
    /// <returns>A sequence of data of <typeparamref name="T"/>; if a basic type (int, string, etc) is queried then the data from the first column is assumed, otherwise an instance is created per row, and a direct column-name===member-name mapping is assumed (case-insensitive). </returns>
    protected async Task<IEnumerable<T>> QueryDbAsync<T>(CommandDefinition command)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        return await connection.QueryAsync<T>(command);
    }




    /// <summary>
    /// Connects and executes a single-row query on the database using the set connection string.
    /// </summary>
    /// <typeparam name="T">The type of data being queried.</typeparam>
    /// <param name="query">The SQL query to be performed.</param>
    /// <param name="param">Parameters to use in <paramref name="query" />.</param>
    /// <param name="cancellationToken">The cancellation token for this command.</param>
    /// <returns>An object of <typeparamref name="T"/>, unless it does not exist then <c>null</c>.</returns>
    protected async Task<T?> QueryDbSingleAsync<T>(string query, object? param = null, CancellationToken cancellationToken = default)
    {
        var command = new CommandDefinition(query, param, transaction: transaction, cancellationToken: cancellationToken);
        return await QueryDbSingleAsync<T>(command);
    }




    /// <summary>
    /// Connects and executes a single-row query on the database using the set connection string.
    /// </summary>
    /// <typeparam name="T">The type of data being queried.</typeparam>
    /// <param name="command">The command to perform.</param>
    /// <returns>An object of <typeparamref name="T"/>, unless it does not exist then <c>null</c>.</returns>
    protected async Task<T?> QueryDbSingleAsync<T>(CommandDefinition command)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        return await connection.QuerySingleOrDefaultAsync<T>(command);
    }




    /// <summary>
    /// Connects and executes a command on the database using the set connection string.
    /// </summary>
    /// <param name="query">The SQL query to be performed.</param>
    /// <param name="param">Parameters to use in <paramref name="query" />.</param>
    /// <param name="cancellationToken">The cancellation token for this command.</param>
    /// <returns>The number of rows affected.</returns>
    protected async Task<int> ExecuteSqlAsync(string query, object? param = null, CancellationToken cancellationToken = default)
    {
        var command = new CommandDefinition(query, param, transaction: transaction, cancellationToken: cancellationToken);
        return await ExecuteSqlAsync(command);
    }




    /// <summary>
    /// Connects and executes a command on the database using the set connection string.
    /// </summary>
    /// <param name="command">The command to perform.</param>
    /// <returns>The number of rows affected.</returns>
    protected async Task<int> ExecuteSqlAsync(CommandDefinition command)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        return await connection.ExecuteAsync(command);
    }




    /// <summary>
    /// Executes a function surrounded by a database transaction. If an exception is thrown within <paramref name="func"/>, the transaction is rolled back. If the function successfully executes it will commit the transaction.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="func">The function to execute.</param>
    /// <returns>The result of the function, unless an exception is thrown then <c>null</c>.</returns>
    protected async Task<T?> ExecuteTransactionAsync<T>(Func<Task<T>> func)
    {
        await using var connection = new NpgsqlConnection(_connectionString);

        await connection.OpenAsync();
        _transaction = await connection.BeginTransactionAsync();

        await using (_transaction)
        {
            try
            {
                var result = await func();
                _transaction.Commit();
                return result;
            }
            catch
            {
                _transaction.Rollback();
                return default;
            }
        }
    }



    /// <summary>
    /// Executes a function surrounded by a database transaction. If an exception is thrown within <paramref name="func"/>, the transaction is rolled back. If the function successfully executes it will commit the transaction.
    /// </summary>
    /// <param name="func">The function to execute.</param>
    /// <returns><c>true</c> if successful, otherwise <c>false</c>.</returns>
    protected async Task<bool> ExecuteTransactionAsync(Func<Task> func)
    {
        return await ExecuteTransactionAsync(async () =>
        {
            await func();
            return true;
        });
    }


    /// <summary>
    /// Connects and executes a query over batches of a dataset using the set connection string.
    /// </summary>
    /// <typeparam name="T">The type of the dataset.</typeparam>
    /// <param name="query">The SQL query to be performed for each batch.</param>
    /// <param name="data">The data to be iterated.</param>
    /// <param name="param">Parameters to use in <paramref name="query" />.</param>
    /// <returns></returns>
    protected async Task<BatchExecuteResult> BatchExecuteAsync<T>(string query, IEnumerable<T> data, object? param = null)
    {
        await using var connection = new NpgsqlConnection(_connectionString);
        connection.Open();

        var retval = new BatchExecuteResult();

        if (data is null || !data.Any()) return retval;

        int numRecords = data.Count();
        for (int i = 0; i < numRecords; i += BATCH_SIZE)
        {
            var batch = data.Skip(i).Take(BATCH_SIZE).ToList();

            // build parameters
            List<DynamicParameters> parameters;
            if (param is null)
            {
                parameters = batch.Select(b => new DynamicParameters(b)).ToList();
            }
            else
            {
                parameters = batch.Select(b =>
                {
                    var p = new DynamicParameters(b);
                    p.AddDynamicParams(param);
                    return p;
                }).ToList();
            }

            using (var transaction = await connection.BeginTransactionAsync())
            {
                try
                {
                    int result = await connection.ExecuteAsync(query, parameters, transaction);
                    retval.SuccessfulExecutions += result;

                    transaction.Commit();
                }
                catch(PostgresException pgEx)
                {
                    transaction.Rollback();

                    Console.WriteLine($"Batch execute failed: {pgEx.Message}");

                    // fall back to individual executions for this batch
                    foreach (var item in batch)
                    {
                        try
                        {
                            var parameter = new DynamicParameters(item);
                            if (param is not null) parameter.AddDynamicParams(param);

                            await connection.ExecuteAsync(query, parameter);
                            retval.SuccessfulExecutions++;
                        }
                        catch(Exception itemEx)
                        {
                            retval.FailedExecutions++;
                            retval.Errors.Add($"Error executing for {item}: {itemEx.Message}");
                            Console.WriteLine($"Execute error: {itemEx.Message} for {JsonSerializer.Serialize(item)}");
                        }
                    }
                }
            }
        }

        return retval;
    }



    /// <summary>
    /// Result object for <see cref="BatchExecuteAsync{T}(string, IEnumerable{T}, object?)"/>
    /// </summary>
    public class BatchExecuteResult
    {
        public int SuccessfulExecutions { get; set; }
        public int FailedExecutions { get; set; }
        public List<string> Errors { get; set; } = new();
    }    
}

