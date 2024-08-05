using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace Parquet.Producers.Util;

public class ThrottledLogger : ILogger
{
    private readonly ILogger _logger;
    private readonly TimeSpan _interval;
    private readonly Stopwatch _timer = new();

    public ThrottledLogger(ILogger logger, TimeSpan interval)
    {
        _logger = logger;
        _interval = interval;
        _timer.Start();
    }

    public IDisposable? BeginScope<TState>(TState state) where TState : notnull => _logger.BeginScope(state);

    public bool IsEnabled(LogLevel logLevel) => _logger.IsEnabled(logLevel);

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
    {
        if (_timer.Elapsed >= _interval)
        {
            _timer.Restart();
            _logger.Log(logLevel, eventId, state, exception, formatter);
        }
    }
}
