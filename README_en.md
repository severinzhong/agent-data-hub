# agent-data-hub

[English](./README_en.md) | [中文](./README.md)

`agent-data-hub` is the companion source repository for `agent-data-cli`.

It provides:

- curated official source implementations
- the repository-root `sources.json` index consumed by the built-in `data_hub` source from `agent-data-cli`

`agent-data-cli` owns the core protocol, CLI, store, and discovery flow; `agent-data-hub` owns the source implementations and the curated source index.

## Connect It To agent-data-cli

In the `agent-data-cli` repository, point `source_workspace` at this repo:

```bash
uv run -m adc config cli set source_workspace /abs/path/to/agent-data-hub
uv run -m adc source list
uv run -m adc content search --source data_hub --channel official --query xiaohongshu
```

`data_hub` is the lightweight built-in source in core, and it reads the official source index from `sources.json` here.

## Curated Sources

| Source | Channel Search | Content Search | Update | Query | Interact |
| --- | --- | --- | --- | --- | --- |
| `ashare` | ✅ | ❌ | ✅ | ✅ | ❌ |
| `ap` | ❌ | ✅ | ✅ | ✅ | ❌ |
| `avwiki` | ❌ | ✅ | ❌ | ❌ | ❌ |
| `bbc` | ❌ | ✅ | ✅ | ✅ | ❌ |
| `cryptocompare` | ✅ | ❌ | ✅ | ✅ | ❌ |
| `hackernews` | ❌ | ✅ | ✅ | ✅ | ❌ |
| `rsshub` | ✅ | ❌ | ✅ | ✅ | ❌ |
| `sina_finance_724` | ❌ | ❌ | ✅ | ✅ | ❌ |
| `usstock` | ✅ | ❌ | ✅ | ✅ | ❌ |
| `wechatarticle` | ❌ | ✅ | ❌ | ❌ | ❌ |
| `xiaohongshu` | ✅ | ✅ | ✅ | ✅ | ✅ |

## Repository Layout

```text
ashare/
ap/
avwiki/
bbc/
cryptocompare/
hackernews/
rsshub/
sina_finance_724/
usstock/
wechatarticle/
xiaohongshu/
sources.json
```

Each source keeps its own runtime dependencies and optional `init.sh`.

Do not install source runtime dependencies into the `agent-data-cli` core manifest, and do not run `uv add` in the core repository for source-specific packages.

Recommended source-local installation patterns:

```bash
uv pip install -p /abs/path/to/agent-data-cli/.venv/bin/python -r /abs/path/to/source/requirements.txt
bash /abs/path/to/source/init.sh
```

This repository does not track a `tests/` directory by default. If you want local source-side tests during development, keep them in your local workspace only.
