# agent-data-hub

`agent-data-hub` is the source workspace repository paired with `agent-data-cli`.

It is the repository that holds:

- official source implementations
- source-specific tests
- the `data_hub` source catalog and install entrypoint

The core repository stays focused on protocol, CLI, store, discovery, and shared fetch infrastructure.

This repository is expected to contain one source package per direct child directory, for example:

```text
ashare/
bbc/
cryptocompare/
data_hub/
rsshub/
xiaohongshu/
```

Each source package should keep its own runtime dependencies and optional `init.sh`.

## Connect It To `agent-data-cli`

In the core repo:

```bash
uv run -m adc config cli set source_workspace /abs/path/to/agent-data-hub
uv run -m adc source list
uv run -m adc content search --source data_hub --channel official --query xiaohongshu
```

Do not install source runtime dependencies into the `agent-data-cli` core project with `uv add`.
Use source-local installation patterns instead:

```bash
uv pip install -p /abs/path/to/agent-data-cli/.venv/bin/python -r /abs/path/to/source/requirements.txt
bash /abs/path/to/source/init.sh
```

## Tests

If `agent-data-hub` is nested next to `agent-data-cli` during development, run:

```bash
cd /abs/path/to/agent-data-hub
../.venv/bin/python -m unittest discover -s tests -p 'test_*.py' -v
```
