"""Decompose the two completed cohorts separately; reads committed observations only."""
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics as stats

HERE = Path(__file__).resolve().parent


def covariance(a, b):
    ma, mb = stats.mean(a), stats.mean(b)
    return sum((x-ma)*(y-mb) for x, y in zip(a, b))/(len(a)-1)


def decompose(source):
    data = json.loads(source.read_text())
    assert data['final'] and data['observed_n'] == 20
    values = data['ln']
    columns = list(map(list, zip(*values)))
    totals = list(map(sum, values))
    variance = stats.variance(totals)
    low, high = min(range(20), key=totals.__getitem__), max(range(20), key=totals.__getitem__)
    pair_columns = [[row[i]-row[i+1] for i in range(0, 20, 2)] for row in columns]
    pair_totals = [totals[i]-totals[i+1] for i in range(0, 20, 2)]
    pair_variance = stats.variance(pair_totals)
    rows = []
    for q, column in enumerate(columns):
        cov = covariance(column, totals)
        loo = []
        for omitted in range(20):
            sub = [value for i, value in enumerate(column) if i != omitted]
            sub_total = [value for i, value in enumerate(totals) if i != omitted]
            loo.append(covariance(sub, sub_total)/stats.variance(sub_total))
        winner = [1 if tries[q][1] <= tries[q][2] else 2 for tries in data['raw_tries']]
        resources = [data['resources'][i][q][winner[i]] for i in range(20)]
        hot = [data['raw_tries'][i][q][winner[i]] for i in range(20)]
        best, worst = min(range(20), key=hot.__getitem__), max(range(20), key=hot.__getitem__)
        rows.append(dict(query=q, mean_hot_s=stats.mean(hot), min_hot_s=min(hot), max_hot_s=max(hot),
                         variance_ln=stats.variance(column), covariance_with_suite=cov, variance_share=cov/variance,
                         paired_variance_share=covariance(pair_columns[q], pair_totals)/pair_variance,
                         observed_range_contribution=column[high]-column[low],
                         loo_variance_share_min=min(loo), loo_variance_share_max=max(loo),
                         selected_hot_zero_gc_count=sum(r[2] == 0 for r in resources),
                         selected_hot_zero_gc_time=sum(r[3] == 0 for r in resources),
                         hot_extremes=[dict(leg=data['legs'][i]['tag'], wall_s=hot[i], selected_try=winner[i]+1,
                                           resource_cpu_s=resources[i][0], gc_count=resources[i][2],
                                           gc_collection_time_s=resources[i][3],
                                           both_hot_tries=data['raw_tries'][i][q][1:],
                                           both_hot_resources=data['resources'][i][q][1:]) for i in (best, worst)]))
    assert math.isclose(sum(row['covariance_with_suite'] for row in rows), variance, abs_tol=1e-12)
    assert math.isclose(sum(row['paired_variance_share'] for row in rows), 1, abs_tol=1e-12)
    assert math.isclose(sum(row['observed_range_contribution'] for row in rows), max(totals)-min(totals), abs_tol=1e-12)
    ordered = sorted(rows, key=lambda row: row['variance_share'], reverse=True)
    range_ordered = sorted(rows, key=lambda row: row['observed_range_contribution'], reverse=True)
    concentrations = []
    for n in (3, 5, 10):
        chosen = ordered[:n]
        concentrations.append(dict(top_n=n, queries=[r['query'] for r in chosen],
                                   variance_share=sum(r['variance_share'] for r in chosen),
                                   range_top_queries=[r['query'] for r in range_ordered[:n]],
                                   range_share=sum(r['observed_range_contribution'] for r in range_ordered[:n])/(max(totals)-min(totals))))
    own = sum(row['variance_ln'] for row in rows)
    return dict(source=str(source.relative_to(HERE.parent)), source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),
                n=20, suite_variance=variance, individual_variances_sum=own,
                cross_query_covariance_sum=variance-own, cross_query_fraction=(variance-own)/variance,
                suite_sd=stats.stdev(totals), observed_range=max(totals)-min(totals),
                range_high_leg=data['legs'][high]['tag'], range_low_leg=data['legs'][low]['tag'],
                paired_variance=pair_variance, paired_sd=stats.stdev(pair_totals),
                concentrations=concentrations, queries=rows,
                variance_order=[r['query'] for r in ordered],
                range_order=[r['query'] for r in range_ordered],
                paired_order=[r['query'] for r in sorted(rows, key=lambda r: r['paired_variance_share'], reverse=True)])


def main():
    studies = {name: decompose(HERE.parent/directory/'summary.json')
               for name, directory in [('full', 'quiet50-20260908'), ('split', 'split50-20260908')]}
    result = dict(method='For S=sum(q), attribution(q)=Cov(q,S)/Var(S); pair differences decomposed identically. Range attribution uses the fixed observed maximum/minimum suite legs.',
                  caveats='Descriptive attribution, not causality or variance removable by fixing one query. Negative shares reflect cancellation. Post-hoc endpoint selection is not an uncertainty interval. Leave-one-out ranges are sensitivity checks, not confidence intervals. Protocols are never pooled.',
                  cohorts=studies)
    (HERE/'summary.json').write_text(json.dumps(result, indent=2, allow_nan=False)+'\n')
    for name, study in studies.items():
        rows = [{k: v for k, v in study['queries'][q].items() if k != 'hot_extremes'} for q in study['variance_order']]
        with (HERE/f'{name}-queries.csv').open('w', newline='') as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator='\n')
            writer.writeheader(); writer.writerows(rows)
        print(name, 'variance', study['suite_variance'], 'cross fraction', study['cross_query_fraction'])
        print('top10:', [(q, round(100*study['queries'][q]['variance_share'], 2)) for q in study['variance_order'][:10]])
        print('paired top10:', [(q, round(100*study['queries'][q]['paired_variance_share'], 2)) for q in study['paired_order'][:10]])


if __name__ == '__main__':
    main()
