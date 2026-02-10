# Run CLI

## Demo

Try the built-in demo to see status tracking with KV parsing in action:

```bash
python -m runtools.runcli job --id demo-status -k python -m runtools.runcli.demo_status
```

The `-k` flag enables KV parsing, which extracts status fields from the output in `key=[value]` format. The demo script simulates a job that goes through init, download (with progress), processing, and reports a final result.
