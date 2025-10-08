import yaml, copy, pathlib

def load(p): return yaml.safe_load(open(p, 'r', encoding='utf-8'))

def deep_merge(a, b):
    if isinstance(a, dict) and isinstance(b, dict):
        out = copy.deepcopy(a)
        for k, v in b.items():
            out[k] = deep_merge(out[k], v) if k in out else copy.deepcopy(v)
        return out
    return copy.deepcopy(b)

def get_fragment(cat, dotted):
    node = cat['fragments']
    for part in dotted.split('.'):
        node = node[part]
    return copy.deepcopy(node)

def subst(obj, mapping):
    if isinstance(obj, dict):
        return {k: subst(v, mapping) for k, v in obj.items()}
    if isinstance(obj, list):
        return [subst(x, mapping) for x in obj]
    if isinstance(obj, str):
        for k, v in mapping.items():
            obj = obj.replace(f"${{{k}}}", str(v))
        return obj
    return obj

def build_recipe(rec_path, cat, out_dir):
    rec = load(rec_path)
    job_name = rec['job_name']
    mapping = {'JOB_NAME': job_name}
    mapping.update(rec.get('variables', {}))
    rec = subst(rec, mapping)

    # single job per recipe
    jobs = rec['resources']['jobs']
    if len(jobs) != 1:
        raise ValueError(f"{rec_path} must define exactly one job")
    job_key, job_def = list(jobs.items())[0]

    merged = {}
    for dotted in rec.get('use', []):
        merged = deep_merge(merged, get_fragment(cat, dotted))
    job_def = deep_merge(merged, job_def)

    out = {'resources': {'jobs': {job_name: job_def}}}
    out_path = pathlib.Path(out_dir) / f"{job_name}.yml"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    yaml.safe_dump(out, open(out_path, 'w', encoding='utf-8'), sort_keys=False)

def main():
    root = pathlib.Path(__file__).resolve().parents[1]
    catalog = load(root / "fragments" / "catalog.yml")
    tasks_dir = root / "tasks"
    out_dir = root / "bundles" / "jobs"
    for p in tasks_dir.glob("*.yml"):
        build_recipe(p, catalog, out_dir)

if __name__ == "__main__":
    main()

