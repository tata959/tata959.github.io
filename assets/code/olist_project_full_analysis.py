from pathlib import Path
import sqlite3
import pandas as pd
import numpy as np


DATA_DIR = Path("C:/Users/10981/Documents/New project/data")
OUT_DIR = Path("C:/Users/10981/Documents/New project/try_analysis")


def clean_channel(x: str) -> str:
    if pd.isna(x):
        return "unknown"
    s = str(x).strip().lower()
    if s in {"", "nan", "none", "null"}:
        return "unknown"
    return s


def build_master() -> pd.DataFrame:
    mql = pd.read_csv(DATA_DIR / "olist_marketing_qualified_leads_dataset.csv")
    cd = pd.read_csv(DATA_DIR / "olist_closed_deals_dataset.csv")
    orders = pd.read_csv(DATA_DIR / "olist_orders_dataset.csv")
    items = pd.read_csv(DATA_DIR / "olist_order_items_dataset.csv")

    orders["order_purchase_ts"] = pd.to_datetime(orders["order_purchase_timestamp"], errors="coerce")
    cd["won_date"] = pd.to_datetime(cd["won_date"], errors="coerce")
    mql["first_contact_date"] = pd.to_datetime(mql["first_contact_date"], errors="coerce")
    mql["channel"] = mql["origin"].map(clean_channel)

    items["line_gmv"] = items["price"] + items["freight_value"]
    order_seller = items.groupby(["order_id", "seller_id"], as_index=False)["line_gmv"].sum()
    order_seller = order_seller.merge(orders[["order_id", "order_purchase_ts"]], on="order_id", how="left")

    cd_base = cd[["mql_id", "seller_id", "won_date"]].copy()
    merged = cd_base.merge(order_seller, on="seller_id", how="left")
    in_window = (
        (merged["order_purchase_ts"] >= merged["won_date"])
        & (merged["order_purchase_ts"] < merged["won_date"] + pd.Timedelta(days=90))
    )
    mql_gmv90 = (
        merged[in_window]
        .groupby("mql_id", as_index=False)["line_gmv"]
        .sum()
        .rename(columns={"line_gmv": "gmv_90d"})
    )

    top12 = set(mql["landing_page_id"].value_counts().head(12).index)
    mql["campaign"] = np.where(
        mql["landing_page_id"].isin(top12),
        mql["landing_page_id"].astype(str).str[:8],
        "other_lp",
    )

    master = (
        mql[["mql_id", "first_contact_date", "channel", "campaign"]]
        .merge(cd_base[["mql_id"]].assign(converted=1), on="mql_id", how="left")
        .merge(mql_gmv90, on="mql_id", how="left")
    )
    master["converted"] = master["converted"].fillna(0).astype(int)
    master["gmv_90d"] = master["gmv_90d"].fillna(0.0)
    return master


def sql_metrics(master: pd.DataFrame):
    conn = sqlite3.connect(":memory:")
    master.to_sql("tmp_master", conn, index=False, if_exists="replace")

    funnel = pd.read_sql_query(
        """
        SELECT
          COUNT(*) AS mql_leads,
          SUM(converted) AS won_deals,
          ROUND(100.0 * SUM(converted) / COUNT(*), 2) AS lead_to_won_cvr_pct,
          SUM(CASE WHEN gmv_90d > 0 THEN 1 ELSE 0 END) AS won_with_gmv,
          ROUND(100.0 * SUM(CASE WHEN gmv_90d > 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS lead_to_pay_cvr_pct
        FROM tmp_master
        """,
        conn,
    )

    channel = pd.read_sql_query(
        """
        SELECT
          channel,
          COUNT(*) AS leads,
          SUM(converted) AS wins,
          ROUND(SUM(gmv_90d), 2) AS gmv_90d,
          ROUND(100.0 * SUM(converted) / COUNT(*), 2) AS cvr_pct,
          ROUND(SUM(gmv_90d) / COUNT(*), 2) AS roi_per_unit,
          ROUND(CASE WHEN SUM(converted) > 0 THEN 1.0 * COUNT(*) / SUM(converted) END, 2) AS cac_unit
        FROM tmp_master
        GROUP BY channel
        HAVING COUNT(*) >= 50
        ORDER BY roi_per_unit DESC
        """,
        conn,
    )

    campaign = pd.read_sql_query(
        """
        SELECT
          campaign,
          COUNT(*) AS leads,
          SUM(converted) AS wins,
          ROUND(SUM(gmv_90d), 2) AS gmv_90d,
          ROUND(100.0 * SUM(converted) / COUNT(*), 2) AS cvr_pct,
          ROUND(SUM(gmv_90d) / COUNT(*), 2) AS roi_per_unit
        FROM tmp_master
        GROUP BY campaign
        ORDER BY roi_per_unit DESC
        LIMIT 20
        """,
        conn,
    )
    return funnel, channel, campaign


def attribution_compare(master: pd.DataFrame) -> pd.DataFrame:
    df = master[["mql_id", "channel", "converted"]].copy()
    last = df[df["converted"] == 1].groupby("channel")["mql_id"].count().rename("wins").reset_index()
    last["last_click_share"] = last["wins"] / last["wins"].sum()

    base = df["converted"].mean()
    n = len(df)
    rows = []
    for ch, grp in df.groupby("channel"):
        wo = df[df["channel"] != ch]
        cvr_wo = wo["converted"].mean()
        traffic_share = len(grp) / n
        removal_effect = (base - cvr_wo) * traffic_share
        rows.append([ch, len(grp), grp["converted"].sum(), grp["converted"].mean(), removal_effect])

    rem = pd.DataFrame(rows, columns=["channel", "leads", "wins", "cvr", "removal_effect"])
    rem["effect_pos"] = rem["removal_effect"].clip(lower=0)
    rem["markov_like_share"] = rem["effect_pos"] / (rem["effect_pos"].sum() or 1)
    out = rem.merge(last[["channel", "last_click_share"]], on="channel", how="left").fillna(0)
    out["delta_pp"] = (out["markov_like_share"] - out["last_click_share"]) * 100
    out = out.sort_values("delta_pp", ascending=False)
    return out


def budget_sim(channel: pd.DataFrame, attr: pd.DataFrame):
    sim = channel.copy()
    for c in ["leads", "roi_per_unit"]:
        sim[c] = pd.to_numeric(sim[c], errors="coerce")
    sim = sim[sim["leads"] >= 100].copy()

    sim["budget"] = sim["leads"].astype(float)  # unit-budget assumption
    sim["new_budget"] = sim["budget"]
    sim["gmv_per_budget"] = sim["roi_per_unit"]

    low = sim.sort_values("gmv_per_budget").head(3)["channel"].tolist()
    high = sim.sort_values("gmv_per_budget", ascending=False).head(3)["channel"].tolist()

    freed = 0.0
    for ch in low:
        b = float(sim.loc[sim["channel"] == ch, "budget"].iloc[0])
        cut = 0.2 * b
        sim.loc[sim["channel"] == ch, "new_budget"] = b - cut
        freed += cut

    w = dict(zip(attr["channel"], attr["markov_like_share"]))
    top_weights = {ch: float(w.get(ch, 0.0)) for ch in high}
    s = sum(top_weights.values())
    if s == 0:
        top_weights = {ch: 1 / len(high) for ch in high}
    else:
        top_weights = {k: v / s for k, v in top_weights.items()}

    for ch in high:
        sim.loc[sim["channel"] == ch, "new_budget"] += freed * top_weights[ch]

    def projected_gmv(row):
        b0, b1, r = row["budget"], row["new_budget"], row["gmv_per_budget"]
        if b1 <= 1.15 * b0:
            return b1 * r
        extra = b1 - 1.15 * b0
        return 1.15 * b0 * r + extra * r * 0.85

    sim["base_gmv"] = sim["budget"] * sim["gmv_per_budget"]
    sim["proj_gmv"] = sim.apply(projected_gmv, axis=1)
    base_total = float(sim["base_gmv"].sum())
    proj_total = float(sim["proj_gmv"].sum())
    uplift = (proj_total - base_total) / base_total * 100 if base_total else 0
    return sim, low, high, base_total, proj_total, uplift


def render_report(funnel, channel, campaign, attr, sim, low, high, base_total, proj_total, uplift):
    f = funnel.iloc[0]
    top_ch = channel.head(5).copy()
    top_ch["roi_per_unit"] = top_ch["roi_per_unit"].astype(float).round(2)
    top_ch["cac_unit"] = top_ch["cac_unit"].astype(float).round(2)

    attr_top = attr[["channel", "last_click_share", "markov_like_share", "delta_pp"]].copy()
    attr_top["last_click_share"] = (attr_top["last_click_share"] * 100).round(2)
    attr_top["markov_like_share"] = (attr_top["markov_like_share"] * 100).round(2)
    attr_top["delta_pp"] = attr_top["delta_pp"].round(2)

    top_ch_txt = top_ch.to_string(index=False)
    attr_top_txt = attr_top.head(10).to_string(index=False)

    report = f"""# 电商投放转化漏斗与多触点归因优化（Olist）

## 1. 项目目标与业务问题
- 目标：回答“预算该投哪、为什么有线索但没形成有效支付GMV”。
- 方法：SQL做漏斗与效率指标，Python做归因对比和预算重分配模拟。

## 2. 数据与字段
- 数据源：Olist真实匿名商业数据（MQL、Closed Deals、Orders、Order Items）。
- 核心字段映射：
  - `user_id` -> `mql_id`
  - `channel` -> `origin`（清洗后）
  - `campaign` -> `landing_page_id`（Top12单独保留，其余other_lp）
  - `gmv` -> `price + freight_value`
  - 下单时间 -> `order_purchase_timestamp`
  - 成交时间 -> `won_date`
- 说明：该数据不含原始曝光/点击日志，因此漏斗口径采用 `MQL -> Won -> 90天内有GMV`。

## 3. SQL阶段：做了什么、为什么这样做
1. 构建 `tmp_order_seller_gmv`
   - 目的：把交易额统一到 seller 维度，便于和 closed_deals 的 `seller_id` 关联。
2. 构建 `tmp_mql_gmv90`
   - 目的：计算每个成交线索在 `won_date` 后90天内产生的真实GMV。
3. 构建 `tmp_master`
   - 目的：形成统一分析底表（channel/campaign/converted/gmv_90d）。
4. 产出三类SQL结果：
   - 漏斗汇总
   - 渠道ROI/CAC
   - Campaign效率

### SQL结果（真实跑数）
- MQL线索数：**{int(f['mql_leads'])}**
- 成交线索数：**{int(f['won_deals'])}**
- Lead->Won CVR：**{f['lead_to_won_cvr_pct']:.2f}%**
- 90天内有GMV的线索数：**{int(f['won_with_gmv'])}**
- Lead->Pay(90d GMV>0) CVR：**{f['lead_to_pay_cvr_pct']:.2f}%**

渠道效率Top（按ROI_unit）：

```text
{top_ch_txt}
```

## 4. Python阶段：做了什么、为什么这样做
1. 归因对比：Last Click vs Markov-like移除效应
   - Last Click：按最终成交计入渠道贡献。
   - Markov-like：看“移除某渠道后整体转化率变化”，衡量增量贡献。
2. 预算重分配模拟
   - 从低ROI渠道各下调20%预算，转移到高ROI+高贡献渠道。
   - 对超15%增量预算加入0.85边际弹性，避免线性高估。

### 归因结果（关键）
```text
{attr_top_txt}
```

## 5. 预算方案与最终结果
- 下调预算渠道（低ROI）：**{", ".join(low)}**
- 增配预算渠道（高ROI+高贡献）：**{", ".join(high)}**
- 基线GMV（单位预算口径）：**{base_total:,.2f}**
- 重分配后GMV：**{proj_total:,.2f}**
- 模拟ROI提升：**{uplift:.2f}%**

## 6. 交付物清单
- 渠道归因模型说明：本报告第4部分 + `attribution_compare.csv`
- 预算重分配模拟表：`budget_simulation.csv`
- 归因可视化输入数据：`attribution_compare.csv`

## 7. 风险与口径说明
- 该数据集没有曝光/点击事件级日志，无法做严格多触点路径Markov。
- 因此使用“Markov思想的移除效应”作为可解释替代；结果用于策略优先级，不等同线上A/B实测增益。
"""
    (OUT_DIR / "analysis_report.md").write_text(report, encoding="utf-8")


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    master = build_master()

    funnel, channel, campaign = sql_metrics(master)
    attr = attribution_compare(master)
    sim, low, high, base_total, proj_total, uplift = budget_sim(channel, attr)

    master.to_csv(OUT_DIR / "tmp_master_preview.csv", index=False)
    funnel.to_csv(OUT_DIR / "funnel_summary.csv", index=False)
    channel.to_csv(OUT_DIR / "channel_metrics.csv", index=False)
    campaign.to_csv(OUT_DIR / "campaign_metrics.csv", index=False)
    attr.to_csv(OUT_DIR / "attribution_compare.csv", index=False)
    sim.to_csv(OUT_DIR / "budget_simulation.csv", index=False)

    render_report(funnel, channel, campaign, attr, sim, low, high, base_total, proj_total, uplift)

    print("Done.")
    print("Report:", OUT_DIR / "analysis_report.md")
    print(f"ROI uplift (simulated): {uplift:.2f}%")


if __name__ == "__main__":
    main()
