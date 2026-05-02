import json, glob

files = sorted(glob.glob('/root/binance-square-agent/data/pipeline_result_round*.json'))
for f in files[-15:]:
    d = json.load(open(f))
    rnum = f.split('round')[1].split('.json')[0]
    steps = d.get('steps', {})
    approved = d.get('approved', False)
    errors = d.get('errors', [])
    harvest = steps.get('harvest', {})
    items = harvest.get('total_items', 0) if isinstance(harvest, dict) else 0
    parse = steps.get('parse', {})
    parse_status = parse.get('status', '?') if isinstance(parse, dict) else '?'
    print(f'轮{rnum:>3}: harvest={items} parse={parse_status} 审批={approved} 错误={len(errors)}')
