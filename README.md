# agent-data-hub

[English](./README_en.md) | [中文](./README.md)

`agent-data-hub` 是 `agent-data-cli` 的伴生 source 仓库。

它负责提供：

- 我已经整理好的官方 source 实现
- 仓库根的 `sources.json` 索引文件，供 `agent-data-cli` 内置的 `data_hub` source 读取

`agent-data-cli` 负责协议、CLI、store、discovery 等 core；`agent-data-hub` 负责 source 本身。

## 连接到 agent-data-cli

在 `agent-data-cli` 仓库里，把 `source_workspace` 指到这个仓库：

```bash
uv run -m adc config cli set source_workspace /abs/path/to/agent-data-hub
uv run -m adc source list
uv run -m adc content search --source data_hub --channel official --query xiaohongshu
```

`data_hub` 是 core 内置的轻量 source，它会从这里的 `sources.json` 读取官方 source 索引。

## 当前整理好的 Source

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

## 仓库结构

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

每个 source 自己维护自己的 runtime 依赖和可选 `init.sh`。

不要把 source 依赖装进 `agent-data-cli` 的 core manifest 里，不要在 core 仓库执行 `uv add`。

推荐的 source 本地安装方式：

```bash
uv pip install -p /abs/path/to/agent-data-cli/.venv/bin/python -r /abs/path/to/source/requirements.txt
bash /abs/path/to/source/init.sh
```

本仓库默认不跟踪测试目录；本地开发时如需保留测试，可自行在工作区维护。
