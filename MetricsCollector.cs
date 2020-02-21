using System;
using System.Linq;

namespace FhirLoader
{
    ///<summary>
    /// Simple class for collecting events.
    /// Contains a circular buffer used to bin events.
    ///</summary>
    public class MetricsCollector
    {
        private readonly object _metricsLock = new object();
        private int _bins = 0;
        private int _resolutionMs = 1000;
        private int _startBin = 0;
        private int _maxBinIndex = 0;
        private DateTime? _startTime = null;
        private long[] _counts;

        ///<summary>
        /// Constructor
        ///</summary>
        public MetricsCollector(int bins = 30, int resolutionMs = 1000)
        {
            _bins = bins;
            _counts = new long[bins];
            _resolutionMs = resolutionMs;
        }

        ///<summary>
        /// Register event at specific time
        ///</summary>
        public void Collect(DateTime eventTime)
        {
            lock (_metricsLock)
            {
                if (_startTime is null)
                {
                    _startTime = DateTime.Now;
                }

                int binIndex = (int)((eventTime - _startTime.Value).TotalMilliseconds / _resolutionMs);

                while (binIndex >= _bins)
                {
                    _counts[_startBin] = 0;
                    _startBin = (_startBin + 1) % _bins;
                    _startTime += TimeSpan.FromMilliseconds(_resolutionMs);
                    binIndex--;
                }

                _counts[(binIndex + _startBin) % _bins]++;

                 // We keep track of this to make sure that in the warm up, we take the average only of bins used
                _maxBinIndex = binIndex;
            }
        }

        ///<summary>
        /// Return events per second
        ///</summary>
        public double EventsPerSecond {
            get {
                lock (_metricsLock)
                {
                    return (double)_counts.Sum() / (_resolutionMs * (_maxBinIndex + 1) / 1000.0);
                }
            }
        }
    }
}