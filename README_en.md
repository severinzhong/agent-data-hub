# agent-data-hub

[English](./README_en.md) | [中文](./README.md)

`agent-data-hub` is the companion source repository for `agent-data-cli`.

It provides:

- curated official source implementations
- the repository-root `sources.json` index consumed by the core `hub` commands from `agent-data-cli`

`agent-data-cli` owns the core protocol, CLI, store, and discovery flow; `agent-data-hub` owns the source implementations and the curated source index.

## Connect It To agent-data-cli

In the `agent-data-cli` repository, point `source_workspace` at this repo:

```bash
adc config cli set source_workspace /abs/path/to/agent-data-hub
adc source list
adc config cli set hub_index /abs/path/to/agent-data-hub/sources.json
adc hub search --query xiaohongshu
```

`hub` is the core command family in `agent-data-cli`, and it reads the official source index from `sources.json` here.

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
| `yahoojp_news` | ✅ | ❌ | ✅ | ✅ | ❌ |

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
yahoojp_news/
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
