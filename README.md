# AstrBot Photon iMessage 插件

让 [AstrBot](https://astrbot.app) 通过 [photon.codes](https://photon.codes) 托管的
**Spectrum** 连接 **iMessage**，接收收到的 iMessage、由 LLM 自动回复，并让其他插件也能响应。

## 为什么需要 Node sidecar

Photon 托管 Spectrum **只有 TypeScript 版 SDK（`spectrum-ts`，基于 gRPC）**，
没有 Python SDK、也没有公开的 HTTP 消息 API。因此本插件内置一个受监督的 **Node sidecar**
做桥接（与 hermes-agent 的 Photon 接入同一套路）：

```
iPhone iMessage ──►  photon.codes Spectrum (gRPC)
                          ▲                     │
                 spectrum-ts (Node sidecar)      │ app.messages 入站流
                          │  loopback HTTP       ▼
                      AstrBot 本插件 ──► 事件队列 ──► 其他插件 + LLM 回复
```

- **入站**：sidecar 把 `app.messages` 流序列化为 NDJSON，通过本地 `GET /inbound` 推给 AstrBot 插件。
- **出站**：AstrBot 插件把回复通过本地 `POST /send` 交给 sidecar，再由 spectrum-ts 发到 iMessage。
- 全程走 **localhost**，**无需公网端口 / webhook**。

## 环境要求

- AstrBot `>=4.16`，Python `>=3.10`
- **Node.js `>=18.17`** 与 **npm**（用于安装并运行 spectrum-ts）
- photon.codes 账号下的 **project_id + project_secret**（你在 hermes 里配置的
  `PHOTON_PROJECT_ID` / `PHOTON_PROJECT_SECRET` 就是这两个值）

## 安装

1. 将插件目录放入 `AstrBot/data/plugins/astrbot_plugin_photon_imessage/`，
   或在 WebUI「插件市场 → 手动安装」上传打包的 zip。
2. 安装 Python 依赖（`requirements.txt`）：`aiohttp>=3.9.0`。
3. 首次运行时插件会**自动在 sidecar 目录执行 `npm install`** 安装 `spectrum-ts@12.7.0`，
   时间较长属正常（也可手动：`cd .../sidecar && npm install`）。此步骤需 AstrBot 机器能访问 npm 源。

## 插件配置（在 WebUI「插件配置」弹窗中填写）

| 配置项 | 说明 | 示例 |
| --- | --- | --- |
| `project_id` | Photon Spectrum 项目 ID (spectrumProjectId) | `photon_proj_xxx` |
| `project_secret` | 项目密钥 (projectSecret)，敏感项 | `your-secret` |
| `sidecar_port` | 本机 sidecar 端口 | `8789`（默认） |
| `sidecar_token` | sidecar 访问令牌（留空则自动生成随机令牌） | 留空 |
| `node_bin` | node 可执行路径（留空自动查找） | 留空 |
| `auto_install_sidecar` | 首次启动自动 `npm install` | `true` |

填写后**保存并重载插件**。日志出现 `photon-sidecar: Spectrum connected` 与
`[Photon] 插件已运行` 即成功。

## 使用

- 配置并重载后，任意非机器人发出的 iMessage 会被提交给 AstrBot，交由 LLM 处理并自动回复到同一会话。
- 你安装的其他插件（指令、监听器等）也能响应 iMessage——它们看到的是一个普通的私聊消息事件。
- 日志中应出现 `[Photon] 收到 ...` 与 `[photon-sidecar] ...` 输出。

## WebUI 初始化向导（不用手动敲 CLI）

插件内置一个「**设置向导**」页面，帮你完成 **登录 Photon → 建独立项目 → 获取 Spectrum 凭据 → 注册手机号 → 写入插件配置** 的全流程，完全在 WebUI 里点击完成（与 hermes 相同的光子设备登录方式，只需浏览器授权一次）。

使用方式：

1. 在 AstrBot WebUI 的**插件 → 本插件 → 详情**里，找到并打开 **Photon 设置** 页面。
2. 按页面步骤依次点击：**登录 → 创建独立项目 → 注册手机号 → 写入配置**。
3. 完成后回到插件配置，确认 `project_id` / `project_secret` 已填入；**重载插件** 生效。

> 该项目为给 AstrBot 独立使用而新建（默认项目名 `astrbot-imessage`），不会改动你 hermes 已用的项目。手机号注册使用 Photon 共享 line 池；是否分配到独立号码以向导返回的 "TEXTS ON" 号码为准。
>
> 向导调用的都是 Photon 官方 Dashboard / Spectrum API，插件里用 `aiohttp` 直接请求，不依赖外部 CLI。

## 常见问题

- **sidecar 依赖装不上**：确认 `node`/`npm` 可用且能访问 npm 源；可手动到 `sidecar/` 执行 `npm install`。
- **连接不上 Spectrum**：检查 `project_id`/`project_secret` 是否正确（与 hermes 的
  `PHOTON_PROJECT_ID`/`PHOTON_PROJECT_SECRET` 一致）。
- **收不到消息**：看 `[photon-sidecar]` 日志；若出现入站流错误，多为 Spectrum 上游短时问题，
  sidecar 与插件会自动重连。

## 参考

- 本插件 sidecar 协议与 hermes-agent 的 [photon 插件](https://github.com/NousResearch/hermes-agent)
  `plugins/platforms/photon/` 一致（projectId/projectSecret、NDJSON 入站、HTTP 出站）。
- Spectrum 使用参考 [photon.codes 文档](https://photon.codes/docs/spectrum-ts/getting-started)。

## License

MIT