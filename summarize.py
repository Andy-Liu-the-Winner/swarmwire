"""Aggregate every recorded trial; never select a best run."""
import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path


def aggregate(report):
    groups = defaultdict(list)
    for row in report['results']:
        parameter = row['producers'] if 'requests_per_second' in row else row.get('payload_text_bytes', row.get('payload_bytes', row.get('workers')))
        groups[(row['workload'], parameter, row['mode'])].append(row)
    results = []
    verified = 0
    for (workload, parameter, mode), rows in sorted(groups.items()):
        metric = 'requests_per_second' if 'requests_per_second' in rows[0] else 'p50_us'
        values = [row[metric] for row in rows]
        result = {
            'workload': workload, 'parameter': parameter, 'mode': mode,
            'trials': len(rows), 'metric': metric,
            'median': round(statistics.median(values), 3),
            'min_trial': round(min(values), 3), 'max_trial': round(max(values), 3),
        }
        if metric == 'p50_us':
            result['median_trial_p95_us'] = round(statistics.median([row['p95_us'] for row in rows]), 3)
            result['median_trial_p99_us'] = round(statistics.median([row['p99_us'] for row in rows]), 3)
            if 'first_exchange_us' in rows[0]:
                result['median_first_exchange_us'] = round(statistics.median([row['first_exchange_us'] for row in rows]), 3)
        results.append(result)
        for row in rows:
            verified += row['verified_requests'] if 'verified_requests' in row else row['samples'] * (row['workers'] if workload == 'scatter_gather' else 1)
    return {
        'aggregation': 'median of per-trial medians (or completed-request rates); ranges span every trial; not pooled percentiles or confidence intervals',
        'measured_verified_request_replies': verified,
        'benchmark_rows': len(report['results']), 'summary': results,
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input', nargs='?', default='comparison.json')
    parser.add_argument('--output', default='comparison-summary.json')
    args = parser.parse_args()
    result = aggregate(json.loads(Path(args.input).read_text()))
    Path(args.output).write_text(json.dumps(result, indent=2) + '\n')
    print('Saved', args.output, 'verified pairs:', result['measured_verified_request_replies'])
