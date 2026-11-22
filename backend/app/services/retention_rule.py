"""retention_rule 领域服务：保留规则管理，接收 tier_policy 上游事件并触发回收清理。

跨模块数据流：
    tier_policy → retention_rule: 温层资产关联保留规则
    retention_rule → reclaim_job: 超过保留期触发回收清理

温层保留规则有两种来源：分层策略派生草稿与手工登记。两种来源共用
``build_warm_rule_values`` 做冷却天数、归档路径和源规则字段的转换，
并落成同一种温层保留规则记录。派生草稿以策略编号作为「源规则」，
同一策略重放时只复用/补全既有记录，不会追加新的有效规则。
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "assets")

# 温层保留规则记录里的来源标记，用于区分「分层策略派生」和「手工登记」。
WARM_RULE_SOURCE = "分层策略"
MANUAL_RULE_SOURCE = "手工登记"

# 派生草稿沿用的原有默认值；手工登记保持调用方给出的值。
DERIVED_RULE_DEFAULTS = {
    "压缩策略": "ZSTD",
    "豁免条件": "",
    "执行频率": "monthly",
    "规则版本": "v1",
}
DERIVED_RETENTION_DAYS = "90"

# 派生草稿生成时必须完整落盘的字段；缺任何一项都视为写入中断的中间产物。
DERIVED_REQUIRED_FIELDS = ["规则编号", "目标资产", "保留天数", "归档路径",
                           "压缩策略", "执行频率", "规则版本", "源规则"]


def _text(value: Any) -> str:
    """把字段统一成可比较、可落盘的字符串。"""
    return "" if value is None else str(value).strip()


def build_warm_rule_values(
    values: dict[str, Any] | None = None,
    *,
    asset_id: str | None = None,
    cooling_days: Any = None,
    archive_path: Any = None,
    source_rule: Any = None,
    derived: bool = False,
) -> dict[str, str]:
    """把分层策略或手工登记的字段组装成温层保留规则。

    转换点集中在这里，两个来源共用：

    * 冷却天数（分层策略）→ 保留天数；
    * 归档路径缺失时，派生草稿按目标资产生成 ``archive://<asset>``；
    * 策略编号 / 源规则 → 统一的「源规则」。

    ``derived=False`` 时不做补默认值，保证手工登记的原有结果不变。
    """
    raw = dict(values or {})
    defaults = DERIVED_RULE_DEFAULTS if derived else {}

    asset = _text(raw.get("目标资产")) or _text(asset_id)
    days = _text(raw.get("保留天数")) or _text(raw.get("冷却天数")) or _text(cooling_days)
    if not days and derived:
        days = DERIVED_RETENTION_DAYS

    path = _text(raw.get("归档路径")) or _text(archive_path)
    if not path and derived and asset:
        path = f"archive://{asset}"

    source = (
        _text(raw.get("源规则"))
        or _text(raw.get("来源规则"))
        or _text(raw.get("策略编号"))
        or _text(source_rule)
    )
    return {
        "目标资产": asset,
        "保留天数": days,
        "归档路径": path,
        "压缩策略": _text(raw.get("压缩策略")) or defaults.get("压缩策略", ""),
        "豁免条件": _text(raw.get("豁免条件")),
        "执行频率": _text(raw.get("执行频率")) or defaults.get("执行频率", ""),
        "规则版本": _text(raw.get("规则版本")) or defaults.get("规则版本", ""),
        "源规则": source,
    }


class RetentionRuleService(BaseModuleService):
    MODULE = "retention_rule"
    ENTITY = "保留规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "目标资产", "保留天数", "归档路径", "压缩策略", "豁免条件", "执行频率", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "目标资产", "保留天数"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def __init__(self):
        super().__init__()
        os.makedirs(DATA_DIR, exist_ok=True)

    # ---------- 草稿组装 ----------
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """手工登记入口：沿用原有必填校验和原值，不补派生默认值。"""
        entry, missing = super().create_entry(values)
        if entry is not None:
            # 「源规则」/「来源」不在列表字段里，但需要随记录保存，供两种来源判重。
            entry["源规则"] = build_warm_rule_values(values, derived=False)["源规则"]
            entry["来源"] = _text(values.get("来源")) or MANUAL_RULE_SOURCE
        return entry, missing

    def ensure_derived_draft(
        self,
        *,
        asset_id: str,
        cooling_days: Any = None,
        source_rule: Any = None,
        policy_values: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """为温层策略派生草稿；同一源规则重放时复用并补全，不追加新规则。

        只有目标资产和源规则都完整的策略才值得生成待激活记录；写入中断留下
        的残缺记录会在重放时按同一份转换结果补齐字段。
        """
        normalized = build_warm_rule_values(
            policy_values,
            asset_id=asset_id,
            cooling_days=cooling_days,
            source_rule=source_rule,
            derived=True,
        )
        if not normalized["目标资产"] or not normalized["源规则"]:
            return None

        existing = self._find_derived_draft(
            normalized["目标资产"], normalized["源规则"])
        if existing is not None:
            self._complete_draft_fields(existing, normalized)
            return existing

        rows = store.rows(self.MODULE)
        next_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        entry = {
            "id": next_id,
            "status": self.STATUS_ORDER[0],
            "pending": True,
            "abnormal": False,
            "规则编号": f"RETE-{next_id:04d}",
            **{field: normalized[field] for field in self.LIST_FIELDS if field != "规则编号"},
            "源规则": normalized["源规则"],
            "来源": WARM_RULE_SOURCE,
        }
        rows.append(entry)
        return entry

    def _find_derived_draft(self, asset_id: str, source_rule: str) -> dict[str, Any] | None:
        """按源规则/目标资产定位既有派生草稿，兼容只写了一半的记录。"""
        rows = store.rows(self.MODULE)
        # 1. 完整派生记录：源规则 + 来源都齐全。
        for row in rows:
            if (_text(row.get("源规则")) == source_rule
                    and _text(row.get("来源")) == WARM_RULE_SOURCE):
                return row
        # 2. 写入中断：来源已落盘、源规则还没落盘的派生草稿。
        for row in rows:
            if (_text(row.get("来源")) == WARM_RULE_SOURCE
                    and _text(row.get("目标资产")) == asset_id
                    and not _text(row.get("源规则"))):
                return row
        # 3. 写入中断：源规则已落盘、来源标记缺失。
        for row in rows:
            if (_text(row.get("源规则")) == source_rule and not _text(row.get("来源"))):
                return row
        return None

    def _complete_draft_fields(self, row: dict[str, Any], normalized: dict[str, str]) -> None:
        """只补缺失字段，不覆盖已经登记或已经生效的值。"""
        for field, value in normalized.items():
            if not _text(row.get(field)):
                row[field] = value
        if not _text(row.get("来源")):
            row["来源"] = WARM_RULE_SOURCE
        if not _text(row.get("规则编号")):
            row["规则编号"] = f"RETE-{int(row.get('id', 0)):04d}"
        if not _text(row.get("status")):
            row["status"] = self.STATUS_ORDER[0]
        if row.get("pending") is None:
            row["pending"] = row.get("status") == self.STATUS_ORDER[0]
        if row.get("abnormal") is None:
            row["abnormal"] = False

    # ---------- 生效与回收 ----------
    def activate_rule(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        used = self._resolve_used_fields(entry)
        if used is None:
            return None, f"{self.ENTITY} {entry_id} 关联不完整，不能作为回收依据"

        asset_id = used["目标资产"]
        retention_days = used["保留天数"]

        entry["status"] = "已生效"
        entry["pending"] = False

        # 写入保留规则记录
        self._write_retention_record(asset_id, entry, used)

        # 仍按既有回收口径：保留期 <= 30 天才创建回收任务。
        try:
            days = int(retention_days)
            if days <= 30:
                self._trigger_reclaim_job(asset_id, used)
        except ValueError:
            pass

        return entry, f"{self.ENTITY} {entry_id} 已生效，保留{retention_days}天"

    def _resolve_used_fields(self, entry: dict[str, Any]) -> dict[str, str] | None:
        """确认待激活记录字段齐全，并返回实际使用的温层规则字段。

        这里只看已经落盘的字段，不用默认值替缺失字段兜底，避免把写入中断
        留下的半条记录当成完整的回收依据。
        """
        if self._is_derived(entry):
            # 派生草稿必须保留到源策略的关联，派生时写入的字段也都要在。
            if any(not _text(entry.get(field)) for field in DERIVED_REQUIRED_FIELDS):
                return None
        elif any(not _text(entry.get(field)) for field in self.REQUIRED_FIELDS):
            return None
        return build_warm_rule_values(entry, derived=self._is_derived(entry))

    @staticmethod
    def _is_derived(entry: dict[str, Any]) -> bool:
        """判断记录是否来自分层策略派生（含写入中断的残缺记录）。"""
        return (
            _text(entry.get("来源")) == WARM_RULE_SOURCE
            or bool(_text(entry.get("源规则")))
        )

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "标记生效":
            return self.activate_rule(entry_id)
        return super().run_action(entry_id, action)

    def _write_retention_record(self, asset_id: str, entry: dict[str, Any],
                                used: dict[str, str] | None = None) -> None:
        """落盘实际使用的温层保留规则字段，供回收与追溯消费。"""
        fields = used or build_warm_rule_values(entry, derived=self._is_derived(entry))
        filename = f"retention_rule_{asset_id}.json"
        path = os.path.join(DATA_DIR, filename)
        record = {
            "asset_id": asset_id,
            "rule_id": _text(entry.get("规则编号")),
            "retention_days": fields["保留天数"],
            "archive_path": fields["归档路径"],
            "compression": fields["压缩策略"],
            "exemption": fields["豁免条件"],
            "frequency": fields["执行频率"],
            "source_rule": fields["源规则"],
            "activated_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)

    def _trigger_reclaim_job(self, asset_id: str, rule: dict[str, Any]) -> None:
        reclaim_rows = store.rows("reclaim_job")
        next_id = max((int(r.get("id", 0)) for r in reclaim_rows), default=0) + 1
        reclaim = {
            "id": next_id,
            "status": "待确认",
            "pending": True,
            "abnormal": False,
            "任务编号": f"RECL-{next_id:04d}",
            "目标资产": asset_id,
            "回收范围": f"超出{rule.get('保留天数', '')}天的历史数据",
            "回收方式": "逻辑删除",
            "影响数据量": "待估算",
            "执行时间": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "恢复窗口": "7天",
            "任务状态": "待确认",
        }
        reclaim_rows.append(reclaim)
