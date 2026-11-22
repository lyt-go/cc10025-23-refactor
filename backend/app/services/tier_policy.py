"""tier_policy 领域服务：分层策略管理，接收 asset_catalog 上游发布事件并生成冷存储与预热任务。

跨模块数据流：
    asset_catalog → tier_policy: 资产发布后自动生成分层策略种子
    tier_policy → cold_storage: 策略生效触发冷存储迁移
    tier_policy → hot_prefetch: 高访问频次触发预热回迁
    tier_policy → retention_rule: 分层策略关联保留规则
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.retention_rule import RetentionRuleService
from app.store import store

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "assets")


class TierPolicyService(BaseModuleService):
    MODULE = "tier_policy"
    ENTITY = "分层策略"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["策略编号", "目标资产", "当前层级", "迁移条件", "目标层级", "冷却天数", "访问频次阈值", "策略版本"]
    REQUIRED_FIELDS = ["策略编号", "目标资产", "当前层级"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用策略": "已停用"}
    NEGATIVE_ACTIONS = ["停用策略"]

    def __init__(self):
        super().__init__()
        os.makedirs(DATA_DIR, exist_ok=True)
        self._lineage_dir = os.path.join(os.path.dirname(__file__), "..", "data", "lineage")
        os.makedirs(self._lineage_dir, exist_ok=True)

    def activate_policy(self, entry_id: int) -> tuple[dict[str, Any] | None, str]:
        """策略生效：根据分层方向触发冷存储迁移或预热回迁。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在"

        current_tier = str(entry.get("当前层级", ""))
        target_tier = str(entry.get("目标层级", ""))

        entry["status"] = "已生效"
        entry["pending"] = False

        asset_id = str(entry.get("目标资产", ""))

        # 写入分层策略数据产物
        self._write_policy_record(asset_id, entry)

        # 根据分层方向触发下游动作
        if current_tier == "HOT" and target_tier == "COLD":
            # 热层 → 冷层：创建冷存储任务
            self._trigger_cold_storage(asset_id, entry)
        elif current_tier == "COLD" and target_tier == "HOT":
            # 冷层 → 热层：创建预热回迁任务
            self._trigger_hot_prefetch(asset_id, entry)
        elif target_tier == "WARM":
            # 温层：更新保留规则关联
            self._link_retention_rule(asset_id, entry)

        # 记录审计日志
        self._write_audit_entry(
            entity="分层策略",
            action="生效",
            target_id=str(entry.get("策略编号", "")),
            detail=f"分层策略 {asset_id} {current_tier}→{target_tier} 已生效",
        )

        # 更新血缘图谱中的层级信息
        self._update_lineage_tier(asset_id, target_tier)

        return entry, f"{self.ENTITY} {entry_id} 已生效，{current_tier}→{target_tier}"

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action == "标记生效":
            return self.activate_policy(entry_id)
        return super().run_action(entry_id, action)

    # ---------- 文件 I/O ----------
    def _write_policy_record(self, asset_id: str, entry: dict[str, Any]) -> None:
        filename = f"tier_policy_{asset_id}.json"
        path = os.path.join(DATA_DIR, filename)
        record = {
            "asset_id": asset_id,
            "current_tier": str(entry.get("当前层级", "")),
            "target_tier": str(entry.get("目标层级", "")),
            "migration_condition": str(entry.get("迁移条件", "")),
            "cooling_days": str(entry.get("冷却天数", "")),
            "access_threshold": str(entry.get("访问频次阈值", "")),
            "activated_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)

    def read_policy_record(self, asset_id: str) -> dict[str, Any] | None:
        filename = f"tier_policy_{asset_id}.json"
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    # ---------- 跨模块副作用 ----------
    def _trigger_cold_storage(self, asset_id: str, policy: dict[str, Any]) -> None:
        """创建冷存储迁移任务。"""
        cold_rows = store.rows("cold_storage")
        next_id = max((int(r.get("id", 0)) for r in cold_rows), default=0) + 1
        task = {
            "id": next_id,
            "status": "待调度",
            "pending": True,
            "abnormal": False,
            "任务编号": f"COLD-{next_id:04d}",
            "源资产": asset_id,
            "目标存储": "S3-GLACIER",
            "压缩算法": "ZSTD",
            "迁移量": "估算中",
            "预计耗时": "估算中",
            "迁移状态": "待调度",
            "校验摘要": "",
        }
        cold_rows.append(task)

    def _trigger_hot_prefetch(self, asset_id: str, policy: dict[str, Any]) -> None:
        """创建预热回迁任务。"""
        hot_rows = store.rows("hot_prefetch")
        next_id = max((int(r.get("id", 0)) for r in hot_rows), default=0) + 1
        task = {
            "id": next_id,
            "status": "排队中",
            "pending": True,
            "abnormal": False,
            "任务编号": f"HOT_-{next_id:04d}",
            "冷存位置": f"S3://glacier/{asset_id}",
            "目标分区": f"{asset_id}_hot",
            "回迁量": "估算中",
            "预计耗时": "估算中",
            "优先级": "高",
            "过期策略": "T+30",
            "任务状态": "排队中",
        }
        hot_rows.append(task)

    def _link_retention_rule(self, asset_id: str, policy: dict[str, Any]) -> dict[str, Any] | None:
        """为温层资产派生保留规则草稿，字段转换与手工登记共用一套。

        派生记录以策略编号作为源规则；同一策略重放时复用并补全既有记录，
        不会额外追加一条重复的派生规则。
        """
        return RetentionRuleService().ensure_derived_draft(
            asset_id=asset_id,
            cooling_days=policy.get("冷却天数"),
            source_rule=policy.get("策略编号"),
            policy_values=policy,
        )

    def _update_lineage_tier(self, asset_id: str, target_tier: str) -> None:
        """更新血缘图谱中的分层信息。"""
        lineage_rows = store.rows("lineage_graph")
        for row in lineage_rows:
            if str(row.get("下游资产")) == asset_id:
                row["血缘状态"] = f"Tier:{target_tier}"
                break

    def _write_audit_entry(self, entity: str, action: str, target_id: str,
                           detail: str) -> None:
        audit_rows = store.rows("audit_log")
        next_id = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
        entry = {
            "id": next_id,
            "status": "已记录",
            "pending": False,
            "abnormal": False,
            "记录编号": f"AUDI-{next_id:04d}",
            "操作主体": entity,
            "操作类型": action,
            "操作对象": target_id,
            "操作时间": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "影响范围": detail,
            "审计摘要": detail,
            "归档状态": "已记录",
        }
        audit_rows.append(entry)