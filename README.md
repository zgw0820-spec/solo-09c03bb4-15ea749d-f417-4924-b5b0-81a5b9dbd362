# 单视频 Range 缓存代理

校内代理场景：学生拖动教学视频进度条时，代理复用已经取过的字节，避免重复回源。

基于 FastAPI 实现，两个容器：

| 服务 | 端口（宿主） | 职责 |
| --- | --- | --- |
| `origin` | 18080 | 可换片、可注入故障的本地视频源站 |
| `proxy` | 18000 | 单视频 Range 缓存代理 |

## 一、设计与实现内容

### 1. 只接收单段闭区间

`proxy/app/main.py` 用正则 `^bytes=(\d+)-(\d+)$` 校验 `Range` 头：

- 只接受 `bytes=start-end` 形式的**单段闭区间**；
- 多段（`bytes=0-99,200-299`）、开区间（`bytes=100-`）、后缀形式（`bytes=-500`）、逆序区间一律 `400`；
- 超出资源长度的区间返回 `416`；缺失 `Range` 头返回 `400`。

### 2. 每次请求先查强 ETag，按版本隔离缓存

每个请求先 `HEAD` 源站：

- 读取强 `ETag`（拒绝 `W/` 弱 ETag）和 `Content-Length`；
- 以 ETag 为版本建立独立的 `VersionCache`（`proxy/app/cache.py`），每个版本一份稀疏缓存文件 + 一份"已覆盖闭区间"列表；
- 换片后 ETag 变化，得到全新版本缓存，**新旧版本同偏移的字节不会串用**；
- LRU 控制版本数量（`CACHE_MAX_VERSIONS`），淘汰时同步删除磁盘版本目录。

### 3. 只补取缺失字节，并发缺口去重

`VersionCache.ensure()` 在一把 `plan_lock` 内做原子规划：

1. 用区间运算（`proxy/app/intervals.py`）算出请求区间相对 `covered` 的缺口；
2. 缺口已被某个在途回源任务**完全覆盖** → 直接等待该任务；
3. 与在途任务**部分重叠** → 重叠部分等待原任务，剩余部分新建任务；
4. 无在途任务覆盖 → 新建回源任务。

因此：

- **重叠部分只回源一次**（多个请求共享同一个 future）；
- **互不重叠的缺口各自建立任务，同时回源**，一个缺口慢或失败不取消、不耽误其他缺口，已补齐的缺口照常落盘登记；
- 分片写盘用 `pwrite/pread` 按偏移定位，并发写互不干扰；
- 分片完成时在同一临界区内更新 `covered`、移除在途条目、再唤醒等待者，避免交界处重复回源或读到稀疏空洞。

### 3.5 覆盖区间持久化：强制重启后零回源命中

每个版本目录（`CACHE_DIR/<安全转义后的ETag>/`）内有两个文件：

| 文件 | 内容 |
| --- | --- |
| `data.bin` | 按 HEAD 总长度 `ftruncate` 预占的稀疏数据文件，实际磁盘块随 `pwrite` 增长 |
| `covered.json` | 覆盖记录：强 ETag、视频总长度、已覆盖闭区间列表 |

**登记顺序保证"返回过 206 就一定能跨重启复用"**：

1. 缺口数据 `pwrite` 写盘并 `fsync(data.bin)`；
2. 持有 `plan_lock` 把新 `covered` 写到 `covered.json.tmp.<pid>`、`fsync` 后 `os.replace` 原子替换、再 `fsync` 目录；
3. 内存 `covered` 更新、移除在途条目、唤醒等待者。

`os.replace` + fsync 保证 `kill -9` / 容器强杀后留下的要么是旧完整记录、要么是新完整记录，不会出现半截 JSON。**截断、换片、写盘或登记失败的补取在任何一步抛错即终止，不会新增覆盖**（`OSError` 统一包装成 502，内存与磁盘记录都不变）。

**恢复时不通读视频正文**：代理不预扫缓存卷，首个请求先做源站 `HEAD`，凭 HEAD 拿到的强 ETag 与 `Content-Length` 懒加载对应版本，只做元数据校验：

- 版本目录 / `covered.json` / `data.bin` 任一缺失；
- `data.bin` 文件长度与 HEAD 总长度不符；
- 记录里的 ETag 与 HEAD 强 ETag 不符、总长度不符；
- JSON 损坏、区间字段类型错误或越界；

任一不符即把**整个版本目录删除按冷缓存重建**（计入 `cold_cache_resets`），校验通过则直接复用记录区间（计入 `restored_versions`），区间内字节零回源。请求跨过已存区间时仍由区间运算拆出缺口，只回源补缺口。

`GET /cache/state` 可查看磁盘版本目录、各版本内存覆盖区间、在途缺口与持久化计数。

### 4. 206 与 Content-Range 的保证

只有当请求区间在**当前 ETag 版本**下全部齐全（命中或补齐成功）才返回：

```
206 Partial Content
Content-Range: bytes start-end/total
ETag: "<当前版本强ETag>"
```

### 5. 换片 / 截断报错与失败重试

回源取缺口时（`make_fetcher`）逐字段校验：

- 源站必须回 `206`，`Content-Range` 区间必须与请求一致；
- 响应 `ETag` 必须与当前版本一致，`Content-Range` 总长度必须与 HEAD 一致，否则判定**换片**，抛 `VersionChanged` → 代理返回 `409`，且**不做无意义重试**；
- 实际字节数少于期望 → **回包截断**，抛 `UpstreamError`；
- 截断、连接中断、写盘/登记失败等普通失败在缺口级别自动重试（`FETCH_MAX_RETRIES`，默认 3 次，带退避），重试耗尽返回 `502`；故障恢复后新请求可重新补齐；
- 任何失败路径都不会修改 `covered` 或 `covered.json`，因此失败补取不产生覆盖、重启后仍会重新回源。

### 6. 可换片 / 可注入故障的源站

`origin/app/main.py`：

- 启动时确定性生成 `v1.bin` / `v2.bin` 两个 1 MiB 版本，ETag = 内容 sha256；
- `POST /admin/swap` 原子换片，`GET /stats` 看版本、ETag、区间请求数、峰值并发；
- `POST /admin/fault` 注入故障：`truncate`（持续截断）、`truncate_once`（截断一次）、`truncate_range`（仅截断命中 `fault_range` 的请求，用于验证并行缺口隔离）、`swap_during`（回源途中换片）、`delay`（响应延迟）、`none`（清除）。

## 二、启动（Docker Compose）

```bash
docker compose up --build
```

- 代理：http://localhost:18000
- 源站：http://localhost:18080

停止并清理数据卷：

```bash
docker compose down -v
```

## 三、示例请求

### 1. 取一个闭区间（首次回源补齐）

```bash
curl -i -H 'Range: bytes=1000-1999' http://localhost:18000/video
```

### 2. 再取同一区间（命中缓存，源站不再发生 GET 取字节）

```bash
curl -i -H 'Range: bytes=1000-1999' http://localhost:18000/video
```

### 3. 取包含已缓存段的更大区间（只补缺失部分）

```bash
curl -i -H 'Range: bytes=0-2999' http://localhost:18000/video
```

### 4. 并发重叠区间（重叠只回源一次，非重叠同时回源）

```bash
curl -s -H 'Range: bytes=4000-5999' http://localhost:18000/video -o a.bin &
curl -s -H 'Range: bytes=5000-6999' http://localhost:18000/video -o b.bin &
curl -s -H 'Range: bytes=8000-9999' http://localhost:18000/video -o c.bin &
wait
curl -s http://localhost:18080/stats
```

### 5. 源站换片

```bash
curl -s -X POST http://localhost:18080/admin/swap -H 'Content-Type: application/json' -d '{"version":"v2"}'
curl -i -H 'Range: bytes=1000-1999' http://localhost:18000/video   # 新版本独立回源
```

### 6. 故障注入与重试

```bash
# 截断一次：代理自动重试后成功
curl -s -X POST http://localhost:18080/admin/fault -H 'Content-Type: application/json' -d '{"mode":"truncate_once"}'
curl -i -H 'Range: bytes=20000-20999' http://localhost:18000/video

# 持续截断：重试耗尽返回 502
curl -s -X POST http://localhost:18080/admin/fault -H 'Content-Type: application/json' -d '{"mode":"truncate"}'
curl -i -H 'Range: bytes=30000-30999' http://localhost:18000/video

# 回源途中换片：返回 409
curl -s -X POST http://localhost:18080/admin/fault -H 'Content-Type: application/json' -d '{"mode":"swap_during"}'
curl -i -H 'Range: bytes=40000-40999' http://localhost:18000/video

# 清除故障
curl -s -X POST http://localhost:18080/admin/fault -H 'Content-Type: application/json' -d '{"mode":"none"}'
```

### 7. 统计与健康检查

```bash
curl -s http://localhost:18000/stats        # 含 restored_versions/cold_cache_resets/coverage_persisted
curl -s http://localhost:18000/cache/state  # 磁盘版本目录与各版本覆盖区间
curl -s http://localhost:18080/stats
curl -s http://localhost:18000/healthz
```

### 8. 强制重启后核对持久化命中（用源站计数核对回源）

```bash
# 8.1 取两个不相邻区间，确认覆盖记录落盘
curl -s -H 'Range: bytes=100000-100999' http://localhost:18000/video -o seg1.bin
curl -s -H 'Range: bytes=200000-200999' http://localhost:18000/video -o seg2.bin
docker compose exec proxy ls -l /data/cache/*/

# 8.2 记录源站基线后，强制杀掉代理容器，用同一缓存卷重启
curl -s -X POST http://localhost:18080/admin/reset-stats
docker compose kill proxy
docker compose up -d proxy            # 不使用 -v，proxy-cache 卷原样保留

# 8.3 重启后重放同样的请求：206 正常返回，而源站 range_requests/bytes_sent 保持 0
curl -i -H 'Range: bytes=100000-100999' http://localhost:18000/video
curl -i -H 'Range: bytes=200000-200999' http://localhost:18000/video
curl -s http://localhost:18080/stats  # "range_requests": 0, "bytes_sent": 0
curl -s http://localhost:18000/stats  # restored_versions >= 1, cold_cache_resets == 0

# 8.4 跨过已存区间的请求只补缺口（99500-99999 与 101000-101499 两段）
curl -i -H 'Range: bytes=99500-101499' http://localhost:18000/video
curl -s http://localhost:18080/stats  # range_requests 恰好为 2、bytes_sent 为 1000

# 8.5 冷缓存规则：删除/损坏覆盖记录或改动 data.bin 长度后重启，该版本整体重新回源
docker compose exec proxy sh -c 'rm /data/cache/*/covered.json'
docker compose kill proxy && docker compose up -d proxy
curl -s -H 'Range: bytes=100000-100999' http://localhost:18000/video >/dev/null
curl -s http://localhost:18000/stats  # cold_cache_resets >= 1
```

## 四、本地自测（无需 Docker）

仓库内置虚拟环境依赖安装与两套测试（本机已验证）：

```bash
python3 -m venv .venv
.venv/bin/pip install fastapi uvicorn httpx
.venv/bin/python -I tests/selftest.py   # 47 项功能自测（含强制重启持久化 G 段）
.venv/bin/python -I tests/stress.py     # 40 随机区间并发 + 强杀重启第三轮零回源
```

## 五、实测结果（本机）

### 功能自测 `tests/selftest.py`：47/47 通过

- A 基础取段：首取 206 + 正确 `Content-Range`；二次请求源站 GET 计数零增长（全命中）；大区间只补 0-999、2000-2999 两段缺失，合并后字节与源站一致；
- B 并发去重：三个重叠/非重叠请求并发，源站仅 3 次区间请求、共 5000 字节（并集大小，重叠零重复），峰值并发 3；
- C 换片：换片后强 ETag 变化，新版本独立回源且新旧版本同偏移内容不同；切回旧版本直接命中旧缓存（源站区间请求零增长）；
- D 截断重试：一次性截断自动重试成功（源站区间请求 ≥2）；持续截断重试耗尽返回 502 且错误信息含"截断"；故障恢复后同区间成功；
- E 回源途中换片：ETag 不匹配返回 409 且不重试；换片后新请求按新版本成功；
- F 非法 Range：多段/开区间/后缀/逆序 → 400，越界 → 416，缺失 Range → 400；
- **G 持久化（19 项）**：覆盖记录随版本落盘且含 ETag/总长度/区间；SIGKILL 强杀 + 同缓存卷重启后历史 206 区间零回源命中（源站 `range_requests=0`、`bytes_sent=0`），`restored_versions≥1`；跨已存区间只补两侧缺口（2 次请求共 1000 字节）；覆盖记录缺失、数据文件缺失、文件长度不符、JSON 损坏四种情况均按冷缓存重建并计 `cold_cache_resets`；持续截断的失败补取不新增覆盖，重启后该区间仍重新回源；并行补取时坏缺口 502 不拖累好缺口 206，好缺口重启后零回源、坏缺口故障清除后重新补齐。

### 并发压测 `tests/stress.py`：通过

- 40 个随机闭区间并发，逐字节比对源站：**0 段不一致**；
- 第二轮全部命中：源站新增区间请求 **0**；
- **第三轮：SIGKILL 强杀代理、同一缓存卷重启后重放全部区间，源站新增区间请求仍为 0、字节 0 段不一致，`restored_versions=1`、`cold_cache_resets=0`**；
- 源站峰值并发区间 **41**（非重叠缺口同时回源）。

## 六、待办 / 已知限制

1. **单视频 / 单路径**：目前只代理 `ORIGIN_VIDEO_PATH`（默认 `/video`）一个资源；多视频需要把缓存键扩展为 `(URL, ETag)`。
2. **单实例内存态去重**：并发去重依赖单进程内的 asyncio 锁与 future，多副本部署时需要引入外部协调（如 Redis 锁）或一致性哈希分流；磁盘覆盖记录本身是单写者语义，未做多进程锁。
3. **每次请求一次 HEAD**：恢复与命中判断都依赖请求前的源站 HEAD 核对强 ETag/总长度（不读正文），HEAD 不可用时请求失败；可加短 TTL 的 HEAD 负缓存但会牺牲换片实时性，当前未做。
4. **无鉴权与限流**：源站管理接口 `/admin/*` 与代理 `/cache/state` 仅适合本地/内网联调，生产暴露需加认证。
5. **HTTP 客户端连接池与超时**为固定配置，大并发下可进一步调优 `httpx.Limits`。
6. 稀疏文件按 `Content-Length` 预 `ftruncate` 到完整大小，`du` 看实际块占用、`ls` 看逻辑大小，属预期行为；每个缺口登记一次 `fsync` + 原子替换，极端高频小缺口下存在写放大，可按需改为批量登记。
