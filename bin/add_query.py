#!/usr/bin/env python
import click
import yaml

@click.command()
@click.option('-n', '--new-query-file')
@click.option('-e', '--existing-query-file')
@click.option('-d', '--delta-output-file')
@click.option('-u', '--update-query-file', is_flag=True)
@click.option('-ht', '--hashtag', is_flag=True)
def main(new_query_file, existing_query_file, delta_output_file, hashtag, update_query_file):
    
    if new_query_file:
        with open(new_query_file) as i_f:
            new_queries = i_f.read().splitlines()
    
    if existing_query_file:
        with open(existing_query_file) as q_f:
            existing_queries_list = yaml.full_load(q_f)
    
    existing_queries = [q['query'] for q in existing_queries_list]
    delta_queries = [q.lower() for q in new_queries if not any(q.lower() in x for x in existing_queries)]
    block_words = ['cnn', 'crypto', 'az', 'breaking', 'fact', 'facts', 'fired', 
    'florida', 'foxnews', 'freedom', 'frostedflakes', 'gif', 'gifs', 'giphy', 'joke', 
    'msnbc','notsorry','phoenix', 'political', 'politics', 'racism', 'texas', 'tokyo2020', 'tokyoolympics',
    'tonythetiger', 'world', 'worldwide']
    delta_queries = [q for q in delta_queries if q not in block_words and len(q) > 3]
    if delta_output_file:
        with open(delta_output_file, 'w+') as d_f:
            d_f.writelines(f'{q}\n' for q in delta_queries)
    
    if hashtag:
        delta_queries = ['#' + q for q in delta_queries]
    
    if update_query_file:
        for q in delta_queries:
            existing_queries_list.append({
                "active": True,
                "query": q,
                "since_id": None, 
                "quota_override": None
            })
        with open(existing_query_file, 'w') as q_f:
            yaml.dump(existing_queries_list, q_f)

if __name__ == '__main__':
    main()