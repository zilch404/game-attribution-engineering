# Attribution Rule v1：Identity Priority + Last Touch

## 候选窗口

点击必须处于闭区间：

```text
[activation_time - 7 days, activation_time]
```

激活之后的点击永远不能归给该激活。恰好位于 7 天边界的点击有效。

## 候选过滤

候选必须同时满足：

1. 与激活属于相同 tenant、game 和 app；
2. 点击未被风控或业务规则标记为无效；
3. 与激活至少共享一个规则允许的规范化身份；
4. 在 tenant、game、app 范围内，同一个命名空间 `click_id` 的完全相同副本只计算一次；
5. 同一个 `click_id` 出现冲突内容时返回 `UNKNOWN/QUARANTINED`，不能任意选择。

## 稳定排序

```text
identity_rank ASC
click_time DESC
click_id ASC
```

即强身份优先，同一身份等级内执行 last-touch，相同时间使用稳定 `click_id` 排序。禁止使用 Kafka Offset、处理到达顺序或 HashMap 迭代顺序作为 tie-break。

## 查询完整性

| 候选查询状态 | 有有效点击 | 无有效点击 |
|---|---|---|
| `AUTHORITATIVE` | `ATTRIBUTED / FINAL` | `ORGANIC / FINAL` |
| `PARTIAL / TRUNCATED` | `ATTRIBUTED / PROVISIONAL` | `UNKNOWN / RETRYABLE` |
| `INDEX_NOT_READY / TIMED_OUT / FAILED` | 不返回候选 | `UNKNOWN / RETRYABLE` |
| `NO_SUPPORTED_IDENTITY / PERMANENT_FAILURE` | 不返回候选 | `UNKNOWN / QUARANTINED` |

因此“查询失败”和“确定没有候选”是两个不同的业务事实。

## 修正

- 正常 grace 内由同一激活的再次查询完成 FINALIZATION。
- 超晚点击或规则重算产生新的 CORRECTION 决策。
- FINAL 表示正常迟到边界已过，不表示永远不可修改。
- 已有 FINAL 时，失败查询或 PARTIAL/TRUNCATED 候选只产生 `RETRY_ONLY`，不能覆盖当前事实；只有完整权威快照才能产生修正。
- 冲突副本、永久查询错误和永久不支持的身份进入 `QUARANTINE`，由数据修复或人工审核处理。
- 新决策必须携带更高 `decision_version` 并引用 `supersedes_decision_id`。
