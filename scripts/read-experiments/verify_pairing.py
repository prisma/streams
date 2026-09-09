#!/usr/bin/env python3
"""Check the previously published ratios and intervals against identified pairs."""
import argparse, json, math
p=argparse.ArgumentParser();p.add_argument('comparisons');p.add_argument('published');a=p.parse_args()
rows=json.load(open(a.comparisons)); old=json.load(open(a.published))
categories={'system/final-system-screen':'serial_system','mimalloc/final-mimalloc-screen':'serial_mimalloc','system/final-system-load':'load_system','mimalloc/final-mimalloc-load':'load_mimalloc'}
checked=0
for row in rows:
    if row['campaign'].startswith('ring/'):
        previous=old['ring'][row['candidate']][row['case'].split(':',1)[1]]['versus_control']
    elif row['control']=='off':
        if row['campaign']!='system/final-system-screen' or row['case']!='history:postings-hot':continue
        previous=old['cache_on_vs_off']
    else:
        case=old[categories[row['campaign']]][row['candidate']][row['case']]
        previous={key:case['paired_'+key] for key in ('p50','p99')}
    for key in ('p50','p99'):
        for actual, published in zip([row[key]['ratio'],*row[key]['ci95']],[previous[key]['ratio'],*previous[key]['ci95']]):
            assert math.isclose(actual,published,rel_tol=1e-12,abs_tol=1e-12),(row,key,actual,published)
        checked+=1
print(json.dumps({'matched_published_ratio_and_ci_triplets':checked,'result':'pass','pairing_source':'explicit filename block ID joined to successful execution receipt; not array position'}))
