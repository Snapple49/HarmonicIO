## IRM configuration parameters

This table shows a list of the parameters available to tune the IRM components in HIO, including a description of each parameter and the default value.

| Parameter name   |      Explanation      |  Default value |
|:-:|:-------------|------:|
| `packing_interval` | Interval in seconds between performing the bin packing algorithm | 1 |
| `default_cpu_share` | Initial guess of CPU size of unencountered container images | 0.125 |
| `profiling_interval` | Interval in seconds between how often worker profiler updates queued container requests | 4 |
| `predictor_interval` | Interval in seconds between predicting load and determining scaling action | 1 |
| `lower_rate_limit` | Lower positive threshold for load predictor | 2 |
| `upper_rate_limit` | Upper positive threshold for load predictor | 5 |
| `slowdown_rate` | Negative threshold for load predictor | -2 |
| `queue_size_limit` | Message queue length limit for load predictor | 10 |
| `scaleup_waiting_time` | Cool-down time for load predictor scaleup actions | 10 |
| `large_scaleup_amount` | Large scaleup quantity for load predictor | 2 |
| `small_scaleup_amount` | Small scaleup quantity for load predictor | 1 |
| `container_request_TTL` | Initial time-to-live counter for container requests | 1 |
