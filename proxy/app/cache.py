"""按强 ETag 分版本的字节缓存（覆盖区间持久化，可跨强制重启复用）。

每个 ETag 版本对应一个版本目录，目录内：
- data.bin：按 total_length 预占的稀疏数据文件，pwrite/pread 按偏移定位；
- covered.json：已覆盖闭区间列表 + ETag/总长度记录，原子替换写入。

持久化与恢复规则：
- 只有完整写盘并 fsync 成功的缺口才会登记进 covered，登记与 covered.json
  的原子替换在同一把 plan_lock 临界区内完成，因此凡返回过 206 的区间，
  其覆盖记录必然已落盘，代理被 kill -9 后用同一缓存卷重启仍可命中；
- 恢复时不读视频正文，只凭请求前的源站 HEAD 拿到的强 ETag 与总长度，
  核对 covered.json 记录与 data.bin 文件长度；任何一项不符（记录缺失、
  数据文件缺失、长度不符、ETag 不符、记录损坏）整个版本按冷缓存处理；
- 截断、换片、写盘或登记失败的补取不会新增任何覆盖。

并发规则（与之前一致）：
- 同一版本的并发请求按缺口去重：重叠部分共享同一个回源任务，
  互不重叠的缺口各自建立独立任务，可同时回源；
- 一个缺口失败不影响其他已补齐缺口的落盘登记。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import shutil
from collections import OrderedDict
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from .intervals import merge, missing, subtract_gap

logger = logging.getLogger(__name__)

Range = Tuple[int, int]
# 回源函数：(etag, start, end) -> 该闭区间的完整字节；失败抛异常
Fetcher = Callable[[str, int, int], Awaitable[bytes]]

_SAFE_ETAG = re.compile(r"[^A-Za-z0-9_.-]")
DATA_FILENAME = "data.bin"
META_FILENAME = "covered.json"


def _safe_name(etag: str) -> str:
    return _SAFE_ETAG.sub("_", etag)[:128]


class VersionCache:
    """单个 ETag 版本的缓存。"""

    def __init__(
        self,
        etag: str,
        total_length: int,
        version_dir: str,
        stats: Optional[Dict[str, int]] = None,
    ) -> None:
        self.etag = etag
        self.total_length = total_length
        self.version_dir = version_dir
        self.path = os.path.join(version_dir, DATA_FILENAME)
        self.meta_path = os.path.join(version_dir, META_FILENAME)
        self._stats = stats
        # 规划锁：计算缺口、登记/复用 in-flight 任务、更新 covered 与落盘时持有，
        # 保证去重、状态切换与覆盖登记原子性
        self.plan_lock = asyncio.Lock()
        self.in_flight: Dict[Range, "asyncio.Future[None]"] = {}
        self.fd = -1
        # 恢复持久化覆盖；校验失败则整个版本目录按冷缓存重建
        self.covered = self._open_or_reset()

    # ------------------------------------------------------------------ 恢复

    def _open_or_reset(self) -> List[Range]:
        """打开版本目录并校验持久化状态，不通读视频正文。

        校验依据只有调用方通过源站 HEAD 拿到的强 ETag 与总长度：
        - 目录/记录/数据文件缺失、文件长度不符、记录字段异常 -> 冷缓存；
        - 校验通过则直接复用记录中的覆盖区间，不重新回源。
        """
        try:
            covered = self._load_validated()
        except Exception as exc:  # noqa: BLE001 - 任何异常都退化为冷缓存
            logger.warning(
                "版本 %s 的持久化缓存校验失败，按冷缓存处理: %r", self.etag, exc
            )
            self._wipe_version_dir()
            if self._stats is not None:
                self._stats["cold_cache_resets"] += 1
            covered = []
        os.makedirs(self.version_dir, exist_ok=True)
        self.fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        os.ftruncate(self.fd, self.total_length)
        if covered and self._stats is not None:
            self._stats["restored_versions"] += 1
            logger.info(
                "版本 %s 恢复 %d 段持久化覆盖（共 %d 字节区间记录）",
                self.etag,
                len(covered),
                sum(e - s + 1 for s, e in covered),
            )
        return covered

    def _load_validated(self) -> List[Range]:
        """只做元数据与文件长度校验，绝不读取 data.bin 正文。"""
        if not os.path.isdir(self.version_dir):
            return []
        # 覆盖记录或对应数据文件缺失 -> 冷缓存
        if not os.path.isfile(self.meta_path) or not os.path.isfile(self.path):
            raise RuntimeError("覆盖记录或数据文件缺失")
        # 文件长度必须与 HEAD 核对后的视频总长度完全一致
        actual_size = os.path.getsize(self.path)
        if actual_size != self.total_length:
            raise RuntimeError(
                f"数据文件长度不符：文件 {actual_size} 字节，HEAD 总长度 {self.total_length}"
            )
        with open(self.meta_path, "r", encoding="utf-8") as fh:
            record = json.load(fh)
        if not isinstance(record, dict):
            raise RuntimeError("覆盖记录结构损坏")
        if record.get("etag") != self.etag:
            raise RuntimeError(
                f"覆盖记录 ETag 不符：记录 {record.get('etag')!r}，HEAD {self.etag!r}"
            )
        if record.get("total_length") != self.total_length:
            raise RuntimeError("覆盖记录总长度与 HEAD 不符")
        raw = record.get("covered")
        if not isinstance(raw, list):
            raise RuntimeError("覆盖记录 covered 字段损坏")
        intervals: List[Range] = []
        for item in raw:
            if not isinstance(item, list) or len(item) != 2:
                raise RuntimeError(f"覆盖区间格式损坏: {item!r}")
            start, end = item
            if isinstance(start, bool) or isinstance(end, bool):
                raise RuntimeError(f"覆盖区间类型损坏: {item!r}")
            if not isinstance(start, int) or not isinstance(end, int):
                raise RuntimeError(f"覆盖区间类型损坏: {item!r}")
            if start < 0 or end < start or end >= self.total_length:
                raise RuntimeError(f"覆盖区间越界: {item!r}")
            intervals.append((start, end))
        # 合并既做规范化也顺带容忍相邻/重复记录
        return merge(intervals)

    def _wipe_version_dir(self) -> None:
        """冷缓存：删除整个版本目录，稍后按全新稀疏文件重建。"""
        shutil.rmtree(self.version_dir, ignore_errors=True)

    # -------------------------------------------------------------- 覆盖登记

    def _persist_locked(self, covered: List[Range]) -> None:
        """把 covered 原子写入 covered.json（调用方已持有 plan_lock）。

        tmp 文件 fsync -> os.replace 原子替换 -> 目录 fsync，
        保证进程被强制终止后留下的要么是旧完整记录、要么是新完整记录。
        """
        record = {
            "format": "video-range-cache/1",
            "etag": self.etag,
            "total_length": self.total_length,
            "covered": [[start, end] for start, end in covered],
        }
        payload = json.dumps(
            record, ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        tmp_path = f"{self.meta_path}.tmp.{os.getpid()}.{id(self)}"
        fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            view = memoryview(payload)
            written = 0
            while written < len(payload):
                written += os.pwrite(fd, view[written:], written)
            os.fsync(fd)
        finally:
            os.close(fd)
        try:
            os.replace(tmp_path, self.meta_path)
            dir_fd = os.open(self.version_dir, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            # 替换失败时清理临时文件，再把异常交给上层（不新增覆盖）
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    # -------------------------------------------------------------- 请求规划

    async def ensure(
        self, start: int, end: int, fetcher: Fetcher, max_retries: int
    ) -> None:
        """保证 [start, end] 在本版本缓存中齐全，必要时回源补齐。

        去重规则（全程持有 plan_lock 做原子规划）：
        - 缺口已被某个运行中的任务完全覆盖 -> 直接等待该任务；
        - 与运行中的任务部分重叠 -> 重叠段等待原任务，剩余段另起任务，
          因此重叠字节只会回源一次，互不重叠的缺口可同时回源。
        """
        tasks: List["asyncio.Future[None]"] = []
        async with self.plan_lock:
            # 清理已完成但还没被 finally 移除的条目
            for done_gap in [g for g, f in self.in_flight.items() if f.done()]:
                self.in_flight.pop(done_gap, None)
            gaps = missing(self.covered, start, end)
            for gap in gaps:
                # 规划期间其他分片可能已完成并更新 covered：
                # 每个缺口处理前都按最新 covered 重新裁剪，防止等待不再写入的分片
                gap = self._trim_to_uncovered(gap)
                if gap is None:
                    continue
                # 任务可能在本轮规划中刚完成：剔除后用最新 covered 重算
                inflight = self._find_inflight(gap)
                if inflight is not None and inflight[1].done():
                    self.in_flight.pop(inflight[0], None)
                    inflight = None
                    gap = self._trim_to_uncovered(gap)
                    if gap is None:
                        continue
                if inflight is None:
                    future = asyncio.get_running_loop().create_future()
                    self.in_flight[gap] = future
                    tasks.append(future)
                    asyncio.create_task(
                        self._run_fetch(gap, fetcher, max_retries, future)
                    )
                    continue

                inflight_gap, reusable_future = inflight
                if inflight_gap[0] <= gap[0] and inflight_gap[1] >= gap[1]:
                    # 运行中任务完全覆盖本缺口
                    if reusable_future not in tasks:
                        tasks.append(reusable_future)
                    continue

                # 部分重叠：重叠部分等待原任务，剩余未覆盖部分新建任务
                if reusable_future not in tasks:
                    tasks.append(reusable_future)
                for piece in subtract_gap(gap, inflight_gap):
                    piece = self._trim_to_uncovered(piece)
                    if piece is None:
                        continue
                    # piece 已是减去重叠后的精确缺口：只有完全包含它的在途任务
                    # 才能满足，部分重叠不算（否则会漏掉夹缝中的缺口）
                    cover = self._covering_inflight(piece)
                    if cover is not None and not cover[1].done():
                        if cover[1] not in tasks:
                            tasks.append(cover[1])
                        continue
                    if cover is not None:
                        self.in_flight.pop(cover[0], None)
                        piece = self._trim_to_uncovered(piece)
                        if piece is None:
                            continue
                    piece_future = asyncio.get_running_loop().create_future()
                    self.in_flight[piece] = piece_future
                    tasks.append(piece_future)
                    asyncio.create_task(
                        self._run_fetch(piece, fetcher, max_retries, piece_future)
                    )
        # 规划完成后再等待，避免持有锁期间串行化；
        # 一个缺口失败不取消其他缺口，等所有任务落地后再抛出第一个失败
        if tasks:
            unique_tasks = list(dict.fromkeys(tasks))
            results = await asyncio.gather(*unique_tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result

    def _find_inflight(
        self, gap: Range
    ) -> Optional[Tuple[Range, "asyncio.Future[None]"]]:
        """返回任意与 gap 有重叠的在途任务。"""
        for inflight_gap, future in self.in_flight.items():
            g_start, g_end = gap
            i_start, i_end = inflight_gap
            if i_start <= g_end and i_end >= g_start:
                return inflight_gap, future
        return None

    def _covering_inflight(
        self, gap: Range
    ) -> Optional[Tuple[Range, "asyncio.Future[None]"]]:
        """返回完全包含 gap 的在途任务（部分重叠不算）。"""
        for inflight_gap, future in self.in_flight.items():
            if inflight_gap[0] <= gap[0] and inflight_gap[1] >= gap[1]:
                return inflight_gap, future
        return None

    def _trim_to_uncovered(self, gap: Range) -> Optional[Range]:
        """按最新 covered 裁剪，返回缺口仍未覆盖的部分。"""
        remain = missing(self.covered, gap[0], gap[1])
        if not remain:
            return None
        return remain[0]

    async def _run_fetch(
        self,
        gap: Range,
        fetcher: Fetcher,
        max_retries: int,
        future: "asyncio.Future[None]",
    ) -> None:
        start, end = gap
        try:
            data: Optional[bytes] = None
            last_exc: Optional[BaseException] = None
            for attempt in range(1, max_retries + 1):
                try:
                    data = await fetcher(self.etag, start, end)
                    break
                except VersionChanged:
                    # 源站已换片，重试没有意义，直接向上抛出
                    raise
                except Exception as exc:  # noqa: BLE001 - 回源失败统一重试
                    last_exc = exc
                    logger.warning(
                        "gap fetch failed etag=%s range=%s attempt=%d/%d: %r",
                        self.etag,
                        f"{start}-{end}",
                        attempt,
                        max_retries,
                        exc,
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(min(0.2 * attempt, 1.0))
            if data is None:
                raise UpstreamError(
                    f"回源补齐缺口 {start}-{end} 失败（已重试 {max_retries} 次）: "
                    f"{last_exc!r}"
                )
            if len(data) != end - start + 1:
                raise UpstreamError(
                    f"源站回包截断：期望 {end - start + 1} 字节，实收 {len(data)}"
                )
            # pread/pwrite 按偏移原子定位，多任务并发写互不干扰
            view = memoryview(data)
            try:
                written = 0
                while written < len(data):
                    written += os.pwrite(self.fd, view[written:], start + written)
                # 数据先落盘并 fsync，再登记覆盖：
                # 截断/写盘失败会直接走异常路径，covered 与磁盘记录都不会新增
                os.fsync(self.fd)
                # 与 in_flight 清理、future 完成放在同一把 plan_lock 临界区内：
                # 先登记 covered + 持久化覆盖记录、移除条目，再唤醒等待者，
                # 保证被唤醒的请求重新规划时不会对已完成分片重复回源，
                # 也保证凡返回过 206 的区间，其覆盖记录必然已持久化
                async with self.plan_lock:
                    merged = merge(self.covered + [gap])
                    # 落盘失败则不修改内存 covered，等待者收到异常后可在后续请求中重新补齐
                    await asyncio.to_thread(self._persist_locked, merged)
                    self.covered = merged
                    if self._stats is not None:
                        self._stats["coverage_persisted"] += 1
                    self.in_flight.pop(gap, None)
                    if not future.done():
                        future.set_result(None)
            except OSError as exc:
                # 写盘或覆盖登记失败：统一按上游类错误返回 502，且不新增任何覆盖
                raise UpstreamError(
                    f"缺口 {start}-{end} 缓存写盘/登记失败: {exc!r}"
                ) from exc
        except BaseException as exc:
            async with self.plan_lock:
                self.in_flight.pop(gap, None)
                if not future.done():
                    future.set_exception(exc)

    def read(self, start: int, end: int) -> bytes:
        return os.pread(self.fd, end - start + 1, start)

    def describe(self) -> Dict[str, Any]:
        return {
            "etag": self.etag,
            "total_length": self.total_length,
            "covered": [[s, e] for s, e in self.covered],
            "in_flight": [[s, e] for s, e in self.in_flight],
        }

    def close(self) -> None:
        if self.fd >= 0:
            try:
                os.close(self.fd)
            except OSError:
                pass
            self.fd = -1


class CacheStore:
    """管理所有 ETag 版本的 VersionCache（简单 LRU，超容量淘汰最旧版本）。"""

    def __init__(self, root: str, max_versions: int = 3) -> None:
        self.root = root
        self.max_versions = max_versions
        os.makedirs(root, exist_ok=True)
        self._entries: "OrderedDict[str, VersionCache]" = OrderedDict()
        self._lock = asyncio.Lock()
        # 持久化相关计数，合并进代理 /stats
        self.stats: Dict[str, int] = {
            "restored_versions": 0,
            "cold_cache_resets": 0,
            "coverage_persisted": 0,
        }

    async def get(self, etag: str, total_length: int) -> VersionCache:
        async with self._lock:
            entry = self._entries.get(etag)
            if entry is not None:
                self._entries.move_to_end(etag)
                return entry
            version_dir = os.path.join(self.root, _safe_name(etag))
            # 构造时完成持久化状态校验：不通读正文，只核对 HEAD 的 ETag/总长度
            entry = VersionCache(
                etag=etag,
                total_length=total_length,
                version_dir=version_dir,
                stats=self.stats,
            )
            self._entries[etag] = entry
            while len(self._entries) > self.max_versions:
                old_etag, old_entry = self._entries.popitem(last=False)
                old_entry.close()
                # 同步删除磁盘版本目录，避免同名版本重建时读到残留稀疏文件
                shutil.rmtree(old_entry.version_dir, ignore_errors=True)
                logger.info("淘汰旧版本缓存 etag=%s", old_etag)
            return entry

    async def describe(self) -> Dict[str, Any]:
        async with self._lock:
            versions = [entry.describe() for entry in self._entries.values()]
            disk_dirs = []
            if os.path.isdir(self.root):
                disk_dirs = sorted(
                    name
                    for name in os.listdir(self.root)
                    if os.path.isdir(os.path.join(self.root, name))
                )
            return {
                "root": self.root,
                "disk_version_dirs": disk_dirs,
                "versions": versions,
                "stats": dict(self.stats),
            }

    async def aclose(self) -> None:
        async with self._lock:
            for entry in self._entries.values():
                entry.close()
            self._entries.clear()


class UpstreamError(RuntimeError):
    """源站不可用、回包截断、协议错误等。"""


class VersionChanged(RuntimeError):
    """回源响应的 ETag 与当前版本不一致（源站换片）。"""
