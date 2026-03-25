import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import re
from chinese_calendar import is_workday, is_holiday
import requests
from io import BytesIO

# -------------------------- 配置项 --------------------------
# GitHub 模板文件的原始链接（替换为你的实际模板链接）
GITHUB_TEMPLATE_URL = "https://raw.githubusercontent.com/hihihidoraemon/Advertiser_deal_logic/main/adv_report_template.xlsx"
# 页面基础配置
st.set_page_config(
    page_title="广告数据分析工具",
    page_icon="📊",
    layout="wide"
)


def load_excel_template(excel_path):
    """加载Excel模板的所有sheet数据"""
    sheets = {
        "流水数据": pd.read_excel(excel_path, sheet_name="1--过去30天总流水"),
        "reject规则": pd.read_excel(excel_path, sheet_name="2--reject规则匹配"),
        "广告主匹配": pd.read_excel(excel_path, sheet_name="3--匹配业务负责广告主"),
        "event事件": pd.read_excel(excel_path, sheet_name="4--event事件"),
        "日均目标流水": pd.read_excel(excel_path, sheet_name="5--本月日均目标流水"),
        "预算黑名单": pd.read_excel(excel_path, sheet_name="6--预算黑名单"),
        "流量类型": pd.read_excel(excel_path, sheet_name="7--流量类型")}
    

    # 数据预处理：日期格式转换,后面涉及到大量日期匹配，避免出现错误
    sheets["流水数据"]["Time"] = pd.to_datetime(sheets["流水数据"]["Time"]).dt.date
    sheets["event事件"]["Time"] = pd.to_datetime(sheets["event事件"]["Time"]).dt.date

    
    #用于后续所有预算详细信息的匹配，以下这些维度信息不会随任何日期发生改变
    offer_base_info = sheets["流水数据"].groupby("Offer ID").agg({
    "Adv Offer ID": lambda x: x.bfill().ffill().iloc[0],
    "GEO": lambda x: x.bfill().ffill().iloc[0],
    "App ID": lambda x: x.bfill().ffill().iloc[0],
    "Advertiser": lambda x: x.bfill().ffill().iloc[0],
    "Total Caps": lambda x: x.bfill().ffill().iloc[0],
    "Status": lambda x: x.bfill().ffill().iloc[0],
    'Payin':lambda x: x.bfill().ffill().iloc[0]}).reset_index()
    
    
    offer_base_info.rename(columns={'Offer ID': 'Offer Id'}, inplace=True)

    offer_base_info['Offer Id']=offer_base_info['Offer Id'].astype(str)
    
    return sheets,offer_base_info


def calculate_total_data(sheets):
    """规则1：按广告主计算日均数据波动"""
    flow_df = sheets["流水数据"].copy()
    adv_match_df = sheets["广告主匹配"].copy()
    daily_target_df = sheets["日均目标流水"].copy()
    
    # 步骤a：匹配二级/三级广告主
    flow_df = pd.merge(
        flow_df,
        adv_match_df[["Advertiser", "二级广告主", "三级广告主"]],
        on="Advertiser",
        how="left"
    )
    
    # 获取最新两天日期
    latest_dates = pd.to_datetime(flow_df["Time"], errors="coerce").drop_duplicates().nlargest(2).sort_values().dt.date
    date_new = latest_dates.iloc[1]  # 最新一天
    date_old = latest_dates.iloc[0] # 次新一天
    
    # 步骤b：按三级广告主计算最新两天数据
    def calculate_level3_data(date):
        return flow_df[flow_df["Time"] == date].groupby("三级广告主").agg({
            "Total Revenue": "sum",
            "Total Profit": "sum"
        }).reset_index()
    
    level3_new = calculate_level3_data(date_new)
    level3_old = calculate_level3_data(date_old)
    
    # 计算利润率
    level3_new["利润率"] = level3_new["Total Profit"] / level3_new["Total Revenue"].replace(0, np.nan)
    level3_old["利润率"] = level3_old["Total Profit"] / level3_old["Total Revenue"].replace(0, np.nan)
    
    # 合并两天数据并计算环比
    level3_merged = pd.merge(
        level3_new.rename(columns={"Total Revenue": "最新 Revenue", "Total Profit": "最新 Profit", "利润率": "最新 利润率"}),
        level3_old.rename(columns={"Total Revenue": "次新 Revenue", "Total Profit": "次新 Profit", "利润率": "次新 利润率"}),
        on="三级广告主",
        how="outer"
    ).fillna(0)
    
    # 环比计算（环比 = (最新-次新)/次新）
    level3_merged["Revenue 环比"] = (level3_merged["最新 Revenue"] - level3_merged["次新 Revenue"]) / level3_merged["次新 Revenue"].replace(0, np.nan)
    level3_merged["利润 环比"] = (level3_merged["最新 Profit"] - level3_merged["次新 Profit"]) / level3_merged["次新 Profit"].replace(0, np.nan)
    level3_merged["利润率 环比"] = (level3_merged["最新 利润率"] - level3_merged["次新 利润率"]) / level3_merged["次新 利润率"].replace(0, np.nan)
    
    # 步骤c：计算最新两天总体数据
    total_new = flow_df[flow_df["Time"] == date_new].agg({
        "Total Revenue": "sum",
        "Total Profit": "sum"
    }).to_frame().T
    total_old = flow_df[flow_df["Time"] == date_old].agg({
        "Total Revenue": "sum",
        "Total Profit": "sum"
    }).to_frame().T
    
    total_new["利润率"] = total_new["Total Profit"] / total_new["Total Revenue"].replace(0, np.nan)
    total_old["利润率"] = total_old["Total Profit"] / total_old["Total Revenue"].replace(0, np.nan)
    
    total_merged = pd.merge(
        total_new.rename(columns={"Total Revenue": "最新 Revenue", "Total Profit": "最新 Profit", "利润率": "最新 利润率"}),
        total_old.rename(columns={"Total Revenue": "次新 Revenue", "Total Profit": "次新 Profit", "利润率": "次新 利润率"}),
        how="outer",
        left_index=True,
        right_index=True
    ).fillna(0)
    
    total_merged["三级广告主"] = "总体"
    total_merged["Revenue 环比"] = (total_merged["最新 Revenue"] - total_merged["次新 Revenue"]) / total_merged["次新 Revenue"].replace(0, np.nan)
    total_merged["利润 环比"] = (total_merged["最新 Profit"] - total_merged["次新 Profit"]) / total_merged["次新 Profit"].replace(0, np.nan)
    total_merged["利润率 环比"] = (total_merged["最新 利润率"] - total_merged["次新 利润率"]) / total_merged["次新 利润率"].replace(0, np.nan)
    
    # 步骤d：合并b和c的数据，匹配日均目标流水
    final_total = pd.concat([level3_merged, total_merged], ignore_index=True)
    final_total = pd.merge(
        final_total,
        daily_target_df[["三级广告主", "本月日均目标流水(美金)"]],
        on="三级广告主",
        how="left"
    ).fillna({"本月日均目标流水(美金)": 0})
    
    # 调整列顺序
    final_total = final_total[[
        "三级广告主", "本月日均目标流水(美金)", "最新 Revenue", "次新 Revenue", "Revenue 环比",
        "最新 Profit", "次新 Profit",'利润 环比',"最新 利润率","次新 利润率","利润率 环比"
    ]]
    
    
    rename_map = {
        "三级广告主": "跟进广告主",
        "本月日均目标流水(美金)": "本月日均目标流水(美金)",
        "最新 Revenue": f"{date_new} 总流水(美金)",
        "次新 Revenue": f"{date_old} 总流水(美金)",
        "Revenue 环比": "流水日环比",
        "最新 Profit": f"{date_new} 总利润(美金)",
        "次新 Profit": f"{date_old} 总利润(美金)",
        "利润 环比": "利润日环比",
        "最新 利润率": f"{date_new} 利润率",
        "次新 利润率": f"{date_new} 利润率",
        "利润率 环比": "利润率日环比"
    }
    final_total = final_total.rename(columns={k: v for k, v in rename_map.items() if k in final_total.columns})

    
    return final_total, date_new, date_old



def calculate_budget_fluctuation(sheets,offer_base_info):
    """
    预算日环比波动分析
    参数：
        sheets: 包含【1--过去30天总流水】的字典（key为sheet名，value为DataFrame）
    返回：
        result_df: 格式化后的预算波动分析结果DataFrame
    """
    # ======================
    # 1. 数据预处理（基础兜底+标准化）
    # ======================
    df = sheets["流水数据"].copy()

    # 统一列名映射（适配不同命名）
    rename_map = {
        "Offer ID": "offerid",
        "Adv Offer ID": "adv_offer_id",
        "Advertiser": "advertiser",
        "App ID": "appid",
        "GEO": "country",
        "Total Caps": "total_cap",
        "Total Clicks": "clicks",
        "Total Conversions": "conversions",
        "Total Revenue": "revenue",
        "Total Profit": "profit",
        "Online hour": "online_hour",
        "Status": "status",
        "Affiliate": "affiliate",
        "Time": "time"
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})


    # 数值字段兜底空值为0（核心：无利润也保留数据）
    num_cols = ["clicks", "conversions", "revenue", "profit", "online_hour"]
    df[num_cols] = df[num_cols].fillna(0).astype(float)


    # 提取全局最新/次新日期
    global_unique_dates = sorted(df["time"].unique(), reverse=True)
    if len(global_unique_dates) < 2:
        return pd.DataFrame()
    day_new = global_unique_dates[0]          # 最新一天
    day_old = global_unique_dates[1]          # 次新一天
    day_new_str = str(day_new)
    day_old_str = str(day_old)
    day_7_ago = day_new - timedelta(days=7)

    # ======================
    # 2. Offer维度利润波动计算（含全量网格兜底）
    # ======================
    all_offer_ids = df["offerid"].unique().tolist()
    offer_date_grid = pd.MultiIndex.from_product(
        [all_offer_ids, [day_new, day_old]],
        names=["offerid", "time"]
    ).to_frame(index=False)

    # Offer按日期聚合
    offer_daily = df.groupby(["offerid", "time"]).agg({
        "profit": "sum",
        "revenue": "sum",
        "online_hour": "max"
    }).reset_index()

    # 合并全量网格，无数据填充0
    offer_full = pd.merge(
        offer_date_grid,
        offer_daily,
        on=["offerid", "time"],
        how="left"
    ).fillna({
        "profit": 0.0, "revenue": 0.0, "online_hour": 0.0
    })

    # 拆分最新/次新数据并合并
    o_new = offer_full[offer_full["time"] == day_new].copy().reset_index(drop=True)
    o_old = offer_full[offer_full["time"] == day_old].copy().reset_index(drop=True)
    offer_merge = pd.merge(
        o_new, o_old,
        on="offerid",
        suffixes=("_new", "_old"),
        how="inner"
    )

    # 计算Offer利润变化，筛选波动≥10或≤-10美金的Offer
    offer_merge["profit_change"] = offer_merge["profit_new"].astype(float) - offer_merge["profit_old"].astype(float)
    fluctuated_offers = offer_merge[offer_merge["profit_change"].abs() >= 5.0].copy()

    if fluctuated_offers.empty:
        return pd.DataFrame()

    # ======================
    # 工具函数
    # ======================
    def format_num(x):
        """金额/数值保留2位小数"""
        return round(float(x), 2)

    def format_pct(x):
        """百分比保留1位小数"""
        return f"{round(float(x) * 100, 1)}%"

    def safe_div(a, b):
        """安全除法，避免除以0"""
        a = float(a)
        b = float(b)
        return a / b if b != 0 else 0.0

    def pct_change(new, old):
        """计算变化百分比"""
        new = float(new)
        old = float(old)
        return (new - old) / old * 100 if old != 0 else 0.0

    # ======================
    # 3. 遍历波动Offer，处理Affiliate维度
    # ======================

    offer_base_info.rename(columns={'Offer Id': 'offerid'}, inplace=True)
    
    offer_base_info['offerid'] = offer_base_info['offerid'].astype(int)
    fluctuated_offers = fluctuated_offers.merge(
        offer_base_info,
        on = 'offerid',
        how='left')
    
    
    rows=[]
    
    target_col = 'Total Caps'

    # 步骤1：尝试转换为数值类型，无法转换的变为NaN
    fluctuated_offers[target_col] = pd.to_numeric(fluctuated_offers[target_col], errors='coerce')

    # 步骤2：筛选条件：非数字(NaN) 或 数值≤0
    condition = (fluctuated_offers[target_col].isna()) | (fluctuated_offers[target_col] <= 0)
    
    fluctuated_offers.loc[condition, target_col] = 100

    for _, offer_row in fluctuated_offers.iterrows():


        
        offer_id = offer_row["offerid"]
        profit_change_offer = float(offer_row["profit_change"])  # Offer级利润变化
        df_offer_all = df[df["offerid"] == offer_id].copy()
        status_latest = offer_row["Status"]
        total_cap_latest = offer_row["Total Caps"]
        adv_offer_id= offer_row["Adv Offer ID"]
        advertiser=offer_row["Advertiser"]
        appid=offer_row["App ID"]
        country=offer_row["GEO"]

        est_price = offer_row['Payin']
        

        # 提取该Offer实际有数据的最新一天附属信息
        df_offer_valid = df_offer_all[
            (df_offer_all["revenue"] != 0) | 
            (df_offer_all["profit"] != 0) | 
            (df_offer_all["clicks"] != 0)
        ]
        if not df_offer_valid.empty:
            offer_latest_date = sorted(df_offer_valid["time"].unique(), reverse=True)[0]


        # 筛选该Offer在最新/次新日期的数据
        df_offer = df_offer_all[(df_offer_all["time"] == day_new) | (df_offer_all["time"] == day_old)].copy()

        # ======================
        # 4. Affiliate维度计算
        # ======================
        # 生成Affiliate全量网格
        if not df_offer.empty:
            all_affiliates = df_offer["affiliate"].unique().tolist()
        else:
            all_affiliates = []
        if len(all_affiliates) == 0:
            all_affiliates = ["未知Affiliate"]

        aff_date_grid = pd.MultiIndex.from_product(
            [all_affiliates, [day_new, day_old]],
            names=["affiliate", "time"]
        ).to_frame(index=False)

        # Affiliate按日期聚合
        aff_daily = df_offer.groupby(["affiliate", "time"]).agg({
            "clicks": "sum",
            "conversions": "sum",
            "revenue": "sum",
            "profit": "sum",
            "online_hour": "max",
        }).reset_index()

        # 合并全量网格，无数据填充0
        aff_full = pd.merge(
            aff_date_grid,
            aff_daily,
            on=["affiliate", "time"],
            how="left"
        ).fillna(0.0)

        # 拆分最新/次新并合并
        aff_new = aff_full[aff_full["time"] == day_new].copy().reset_index(drop=True)
        aff_old = aff_full[aff_full["time"] == day_old].copy().reset_index(drop=True)
        aff_merge = pd.merge(
            aff_new, aff_old,
            on="affiliate",
            suffixes=("_new", "_old"),
            how="outer"
        ).fillna(0.0)

        # 计算Affiliate的CR、利润率、变化值/变化率
        aff_merge["cr_new"] = aff_merge.apply(lambda x: safe_div(x["conversions_new"], x["clicks_new"]), axis=1)
        aff_merge["cr_old"] = aff_merge.apply(lambda x: safe_div(x["conversions_old"], x["clicks_old"]), axis=1)
        aff_merge["margin_new"] = aff_merge.apply(lambda x: safe_div(x["profit_new"], x["revenue_new"]), axis=1)
        aff_merge["margin_old"] = aff_merge.apply(lambda x: safe_div(x["profit_old"], x["revenue_old"]), axis=1)

        aff_merge["profit_change"] = aff_merge["profit_new"].astype(float) - aff_merge["profit_old"].astype(float)
        aff_merge["revenue_pct"] = aff_merge.apply(lambda x: pct_change(x["revenue_new"], x["revenue_old"]), axis=1)
        aff_merge["clicks_pct"] = aff_merge.apply(lambda x: pct_change(x["clicks_new"], x["clicks_old"]), axis=1)
        aff_merge["cr_pct"] = aff_merge.apply(lambda x: pct_change(x["cr_new"], x["cr_old"]), axis=1)
        aff_merge["margin_pct"] = aff_merge.apply(lambda x: pct_change(x["margin_new"], x["margin_old"]), axis=1)

        # ======================
        # 5. 核心逻辑：筛选影响的Affiliate并生成文本
        # ======================
        aff_affect = []
        downstream_text = []

        # 场景1：Offer利润下降≤-5美金 → 只关注利润也减少的Affiliate（profit_change≤-5）
        if profit_change_offer <= -5.0:
            # 筛选条件：Affiliate利润变化≤-5美金
            aff_affect = aff_merge[aff_merge["profit_change"] <= -3.0].copy()
            
            for _, arow in aff_affect.iterrows():
                aff_name = arow["affiliate"]
                apc = format_num(arow["profit_change"])  # Affiliate利润变化
                p_old = format_num(arow["profit_old"])    # 次新一天Profit
                p_new = format_num(arow["profit_new"])    # 最新一天Profit
                r_old = format_num(arow["revenue_old"])   # 次新一天Revenue
                r_new = format_num(arow["revenue_new"])   # 最新一天Revenue
                c_old = format_num(arow["clicks_old"])    # 次新一天Clicks
                c_new = format_num(arow["clicks_new"])    # 最新一天Clicks
                cr_old = format_pct(arow["cr_old"])       # 次新一天CR
                cr_new = format_pct(arow["cr_new"])       # 最新一天CR
                m_old = format_pct(arow["margin_old"])    # 次新一天利润率
                m_new = format_pct(arow["margin_new"])    # 最新一天利润率
                rp = f"{round(arow['revenue_pct'], 1)}%"  # Revenue变化%
                cp = f"{round(arow['clicks_pct'], 1)}%"   # Clicks变化%
                crp = f"{round(arow['cr_pct'], 1)}%"      # CR变化%

                # 子场景1：最新一天Profit减少为0
                if float(p_new) == 0 and float(p_old) != 0:
                    reduce_revenue = format_num(float(r_old) - float(r_new))
                    txt = (f"{aff_name} 停止产生流水，减少流水 {reduce_revenue} 美金，"
                           f"对应Total revenue从 {r_old} 美金（{day_old_str}）变为 {r_new} 美金（{day_new_str}）")
                
                # 子场景2：Profit未减少为0（≤-5美金）
                else:
                    txt = (f"{aff_name} 的Total Profit影响 {apc} 美金，"
                           f"对应Total Profit从 {p_old} 美金（{day_old_str}）变为 {p_new} 美金（{day_new_str}）")
                    
                    # 拆解影响因素：流水贡献 vs 利润率贡献
                    rev_contrib = (float(r_new) - float(r_old)) * safe_div(arow["profit_old"], r_old) if float(r_old) != 0 else 0.0
                    margin_contrib = float(r_new) * (safe_div(arow["profit_new"], r_new) - safe_div(arow["profit_old"], r_old)) if float(r_new) != 0 else 0.0
                    rev_contrib = format_num(rev_contrib)
                    margin_contrib = format_num(margin_contrib)
                    total_contrib = abs(float(rev_contrib)) + abs(float(margin_contrib))

                    if total_contrib < 1e-6:
                        factor_txt = ""
                    else:
                        rev_ratio = abs(float(rev_contrib)) / total_contrib  # 流水影响占比
                        margin_ratio = abs(float(margin_contrib)) / total_contrib  # 利润率影响占比

                        # 流水影响超80%
                        if rev_ratio > 0.8:
                            factor_txt = (f"，主要受流水下降影响，影响利润 {rev_contrib} 美金，"
                                         f"Total revenue从 {r_old} 美金变为 {r_new} 美金，变化{rp}，"
                                         f"Total Clicks从 {c_old} 变为 {c_new}，变化{cp}，"
                                         f"CR从 {cr_old} 变为 {cr_new}，变化{crp}")
                        # 利润率影响超80%
                        elif margin_ratio > 0.8:
                            factor_txt = (f"，主要受利润率下降影响，影响利润 {margin_contrib} 美金，"
                                         f"利润率从 {m_old} 变为 {m_new}，"
                                         f"请检查是否价格/预算设置发生改变，导致利润率下降")
                        # 二者共同影响
                        else:
                            factor_txt = (f"，流水和利润率分别影响 {rev_contrib} 美金和 {margin_contrib} 美金，"
                                         f"Total revenue从 {r_old} 美金变为 {r_new} 美金，变化{rp}，"
                                         f"Total Clicks从 {c_old} 变为 {c_new}，变化{cp}，"
                                         f"CR从 {cr_old} 变为 {cr_new}，变化{crp}，"
                                         f"同时利润率从 {m_old} 变为 {m_new}，"
                                         f"请检查是否价格/预算设置发生改变，导致利润率发生变化")
                    txt += factor_txt
                downstream_text.append(txt)

        # 场景2：Offer利润上涨≥10美金 → 只关注利润也增加的Affiliate（profit_change≥5）
        elif profit_change_offer >= 5.0:
            # 筛选条件：Affiliate利润变化≥5美金
            aff_affect = aff_merge[aff_merge["profit_change"] >= 3.0].copy()
            
            for _, arow in aff_affect.iterrows():
                aff_name = arow["affiliate"]
                apc = format_num(arow["profit_change"])  # Affiliate利润变化
                p_old = format_num(arow["profit_old"])    # 次新一天Profit
                p_new = format_num(arow["profit_new"])    # 最新一天Profit
                r_old = format_num(arow["revenue_old"])   # 次新一天Revenue
                r_new = format_num(arow["revenue_new"])   # 最新一天Revenue
                c_old = format_num(arow["clicks_old"])    # 次新一天Clicks
                c_new = format_num(arow["clicks_new"])    # 最新一天Clicks
                cr_old = format_pct(arow["cr_old"])       # 次新一天CR
                cr_new = format_pct(arow["cr_new"])       # 最新一天CR
                m_old = format_pct(arow["margin_old"])    # 次新一天利润率
                m_new = format_pct(arow["margin_new"])    # 最新一天利润率
                rp = f"{round(arow['revenue_pct'], 1)}%"  # Revenue变化%
                cp = f"{round(arow['clicks_pct'], 1)}%"   # Clicks变化%
                crp = f"{round(arow['cr_pct'], 1)}%"      # CR变化%

                # 子场景1：次新一天Profit为0（新增流水）
                if float(p_old) == 0 and float(p_new) != 0:
                    add_revenue = format_num(float(r_new) - float(r_old))
                    txt = (f"{aff_name} 增加产生流水，增加流水 {add_revenue} 美金，"
                           f"对应Total revenue从 {r_old} 美金（{day_old_str}）变为 {r_new} 美金（{day_new_str}）")
                
                # 子场景2：Profit未从0开始（≥5美金）
                else:
                    txt = (f"{aff_name} 的Total Profit影响 {apc} 美金，"
                           f"对应Total Profit从 {p_old} 美金（{day_old_str}）变为 {p_new} 美金（{day_new_str}），"
                           f"Total revenue从 {r_old} 美金（{day_old_str}）变为 {r_new} 美金（{day_new_str}）")
                    
                    # 拆解影响因素：流水贡献 vs 利润率贡献
                    rev_contrib = (float(r_new) - float(r_old)) * safe_div(arow["profit_old"], r_old) if float(r_old) != 0 else 0.0
                    margin_contrib = float(r_new) * (safe_div(arow["profit_new"], r_new) - safe_div(arow["profit_old"], r_old)) if float(r_new) != 0 else 0.0
                    rev_contrib = format_num(rev_contrib)
                    margin_contrib = format_num(margin_contrib)
                    total_contrib = abs(float(rev_contrib)) + abs(float(margin_contrib))

                    if total_contrib < 1e-6:
                        factor_txt = ""
                    else:
                        rev_ratio = abs(float(rev_contrib)) / total_contrib  # 流水影响占比
                        margin_ratio = abs(float(margin_contrib)) / total_contrib  # 利润率影响占比

                        # 流水影响超80%
                        if rev_ratio > 0.8:
                            factor_txt = (f"，主要受流水上涨影响，影响利润 {rev_contrib} 美金，"
                                         f"Total revenue从 {r_old} 美金变为 {r_new} 美金，变化{rp}，"
                                         f"Total Clicks从 {c_old} 变为 {c_new}，变化{cp}，"
                                         f"CR从 {cr_old} 变为 {cr_new}，变化{crp}")
                        # 利润率影响超80%
                        elif margin_ratio > 0.8:
                            factor_txt = (f"，主要受利润率上涨影响，影响利润 {margin_contrib} 美金，"
                                         f"利润率从 {m_old} 变为 {m_new}，"
                                         f"请检查是否价格/预算设置发生改变，导致利润率变化")
                        # 二者共同影响
                        else:
                            factor_txt = (f"，流水和利润率分别影响 {rev_contrib} 美金和 {margin_contrib} 美金，"
                                         f"Total revenue从 {r_old} 美金变为 {r_new} 美金，变化{rp}，"
                                         f"Total Clicks从 {c_old} 变为 {c_new}，变化{cp}，"
                                         f"CR从 {cr_old} 变为 {cr_new}，变化{crp}，"
                                         f"同时利润率从 {m_old} 变为 {m_new}，"
                                         f"请检查是否价格/预算设置发生改变，导致利润率发生变化")
                    txt += factor_txt
                downstream_text.append(txt)

        # 无影响的Affiliate
        if not downstream_text:
            downstream_text = ["无下游Affiliate有明显利润变化"]
        downstream_final = "\n".join(downstream_text)

        # ======================
        # 6. 在线时长/预算状态总结
        # ======================
        oh_new = format_num(offer_row["online_hour_new"])
        oh_old = format_num(offer_row["online_hour_old"])
        oh_diff = format_num(float(offer_row["online_hour_new"]) - float(offer_row["online_hour_old"]))
        
        

        if status_latest == "PAUSE":
            print(1,status_latest)
            status_summary = "预算已暂停，优先询问广告主预算暂停原因"
        elif status_latest == "ACTIVE":
            if float(oh_diff) >= 0 and profit_change_offer <= -10.0:
                print(2,status_latest)
                status_summary = f"在线时长无变化（{day_old_str}：{oh_old}小时 → {day_new_str}：{oh_new}小时），但利润有明显下降，重点沟通影响下游"
            elif float(oh_diff) < -4 and profit_change_offer <= -10.0:
                print(3,status_latest)
                status_summary = f"在线时长减少4小时以上（{day_old_str}：{oh_old}小时 → {day_new_str}：{oh_new}小时），先和广告主沟通预算是否不足"
            else:
                status_summary = ""
                print(4,status_latest)
        else:
            status_summary = ""
            print(5,status_latest)

        # 新/旧预算判断
        if not df_offer_all.empty:
            first_day = df_offer_all["time"].min()
        else:
            first_day = day_new
        budget_type = "新预算" if first_day >= day_7_ago else "旧预算"

        # ======================
        # 7. 组装结果行（金额加美金）
        # ======================
        revenue_new = format_num(offer_row["revenue_new"])
        revenue_old = format_num(offer_row["revenue_old"])
        profit_new = format_num(offer_row["profit_new"])
        profit_old = format_num(offer_row["profit_old"])
        profit_diff = format_num(profit_change_offer)
        cap_latest = format_num(total_cap_latest)

       

        # 利润率
        margin_new = format_pct(safe_div(offer_row["profit_new"], offer_row["revenue_new"]))
        margin_old = format_pct(safe_div(offer_row["profit_old"], offer_row["revenue_old"]))

        rows.append({
            "offer id": offer_id,
            "adv offer id": adv_offer_id,
            "Advertiser": advertiser,
            "appid": appid,
            "country": country,
            f"{day_new_str} Total cap": cap_latest,
            f"Payin": est_price,
            f"{day_new_str} online hour（小时）": oh_new,
            f"{day_old_str} online hour（小时）": oh_old,
            f"{day_new_str} Total Revenue（美金）": revenue_new,
            f"{day_old_str} Total Revenue（美金）": revenue_old,
            f"{day_new_str} Total Profit（美金）": profit_new,
            f"{day_old_str} Total Profit（美金）": profit_old,
            f"{day_new_str} 利润率": margin_new,
            f"{day_old_str} 利润率": margin_old,
            f"Total Profit变化差值（{day_new_str}-{day_old_str}）（美金）": profit_diff,
            f"online hour变化差值（{day_new_str}-{day_old_str}）（小时）": oh_diff,
            "预算status状态": status_latest,
            "在线时长和预算状态总结": status_summary,
            "具体影响下游总结": downstream_final,
            "预算类型": budget_type
        })

    # ======================
    # 8. 结果格式化
    # ======================
    result_df = pd.DataFrame(rows)
    # 确保数值列类型正确
    for col in result_df.columns:
        if "%" in col or "总结" in col or "类型" in col or "状态" in col or "offer id" in col:
            continue
        result_df[col] = pd.to_numeric(result_df[col], errors="ignore")


    return result_df



def calculate_reject_data(sheets):
    """规则3：计算reject数据"""
    event_df = sheets["event事件"].copy()
    reject_rule_df = sheets["reject规则"].copy()
    adv_match_df = sheets["广告主匹配"].copy()
    
    # 步骤a：匹配是否为reject
    event_df = pd.merge(
        event_df,
        reject_rule_df[["Event", "是否为reject"]],
        on="Event",
        how="left"
    ).fillna({"是否为reject": False})
    
    # 步骤b：匹配二级/三级广告主
    event_df = pd.merge(
        event_df,
        adv_match_df[["Advertiser", "二级广告主", "三级广告主"]],
        left_on="Advertiser",
        right_on="Advertiser",
        how="left"
    )

    
    # 步骤c：调整Appnext的Time字段
    event_df.loc[(event_df["是否为reject"] == True) & (event_df["三级广告主"] == "Appnext"), "Time"] -= timedelta(days=1)
    
    return event_df

def calculate_advertiser_data(sheets, date_new, date_old, reject_event_df):
    """规则4：计算Advertiser数据"""
    flow_df = sheets["流水数据"].copy()
    adv_match_df = sheets["广告主匹配"].copy()
    
    flow_df = pd.merge(
        flow_df,
        adv_match_df[["Advertiser", "二级广告主", "三级广告主"]],
        left_on="Advertiser",
        right_on="Advertiser",
        how="left")

    # 步骤a：按二级广告主计算流水和利润数据
    def calculate_adv_revenue_profit(date):
        return flow_df[flow_df["Time"] == date].groupby("二级广告主").agg({
            "Total Revenue": "sum",
            "Total Profit": "sum"
        }).reset_index()
    
    adv_new = calculate_adv_revenue_profit(date_new)
    adv_old = calculate_adv_revenue_profit(date_old)
    
    adv_merged = pd.merge(
        adv_new.rename(columns={"Total Revenue": "最新 Revenue", "Total Profit": "最新 Profit"}),
        adv_old.rename(columns={"Total Revenue": "次新 Revenue", "Total Profit": "次新 Profit"}),
        on="二级广告主",
        how="outer"
    ).fillna(0)
    
    # 计算利润率和变化幅度
    adv_merged["最新 利润率"] = adv_merged["最新 Profit"] / adv_merged["最新 Revenue"].replace(0, np.nan)
    adv_merged["次新 利润率"] = adv_merged["次新 Profit"] / adv_merged["次新 Revenue"].replace(0, np.nan)
    adv_merged["Total Revenue 变化幅度"] = (adv_merged["最新 Revenue"] - adv_merged["次新 Revenue"]) / adv_merged["次新 Revenue"].replace(0, np.nan) 
    adv_merged["Total Profit 变化幅度"] = (adv_merged["最新 Profit"] - adv_merged["次新 Profit"]) / adv_merged["次新 Profit"].replace(0, np.nan) 
    adv_merged["利润率 变化幅度"] = (adv_merged["最新 利润率"] - adv_merged["次新 利润率"]) / adv_merged["次新 利润率"].replace(0, np.nan) 
    
    # 步骤b：计算reject率
    def calculate_reject_count(date, df):
        
        df_filtered = df[(df["Time"] == date) & (df["是否为reject"] == True)]
        return df_filtered.groupby("二级广告主").agg({
        "是否为reject": "count" }).rename(columns={"是否为reject": "Total reject"})
        
    
    
    reject_new = calculate_reject_count(date_new, reject_event_df)
    reject_old = calculate_reject_count(date_old, reject_event_df)
    
    def calculate_conversions(date):
        return flow_df[flow_df["Time"] == date].groupby("二级广告主").agg({
            "Total Conversions": "sum"
        }).reset_index()
    
    conv_new = calculate_conversions(date_new)
    conv_old = calculate_conversions(date_old)
    
    # 合并reject和conversions数据
    adv_reject_new = pd.merge(reject_new, conv_new, on="二级广告主", how="outer").fillna(0)
    adv_reject_old = pd.merge(reject_old, conv_old, on="二级广告主", how="outer").fillna(0)
    
    adv_reject_new["reject率"] = adv_reject_new["Total reject"] / (adv_reject_new["Total reject"] + adv_reject_new["Total Conversions"]).replace(0, np.nan)
    adv_reject_old["reject率"] = adv_reject_old["Total reject"] / (adv_reject_old["Total reject"] + adv_reject_old["Total Conversions"]).replace(0, np.nan)
    
    # 最终合并所有数据
    final_adv = pd.merge(adv_merged, adv_reject_new[["二级广告主", "Total reject", "reject率"]].rename(columns={"Total reject": "最新 Total reject", "reject率": "最新 reject率"}), on="二级广告主", how="outer")
    final_adv = pd.merge(final_adv, adv_reject_old[["二级广告主", "Total reject", "reject率"]].rename(columns={"Total reject": "次新 Total reject", "reject率": "次新 reject率"}), on="二级广告主", how="outer")
    
    
    final_adv.rename(columns={'最新 Revenue': f'{date_new} Total Revenue'}, inplace=True)
    final_adv.rename(columns={'最新 Profit': f'{date_new} Total Profit'}, inplace=True)
    final_adv.rename(columns={'次新 Revenue': f'{date_old} Total Revenue'}, inplace=True)
    final_adv.rename(columns={'次新 Profit': f'{date_old} Total Profit'}, inplace=True)
    final_adv.rename(columns={'最新 利润率': f'{date_new} 利润率'}, inplace=True)
    final_adv.rename(columns={'次新 利润率': f'{date_old} 利润率'}, inplace=True)
    final_adv.rename(columns={'最新 Total reject': f'{date_new} Total reject'}, inplace=True)
    final_adv.rename(columns={'最新 reject率': f'{date_new} reject率'}, inplace=True)
    final_adv.rename(columns={'次新 Total reject': f'{date_old} Total reject'}, inplace=True)
    final_adv.rename(columns={'次新 reject率': f'{date_old} reject率'}, inplace=True)



    
    
    return final_adv.fillna(0)

def calculate_affiliate_data(sheets, date_new, date_old, reject_event_df):
    """规则5：计算Affiliate数据"""
    flow_df = sheets["流水数据"].copy()
    
    # 步骤a：按Affiliate计算流水和利润数据
    def calculate_aff_revenue_profit(date):
        return flow_df[flow_df["Time"] == date].groupby("Affiliate").agg({
            "Total Revenue": "sum",
            "Total Profit": "sum"
        }).reset_index()
    
    aff_new = calculate_aff_revenue_profit(date_new)
    aff_old = calculate_aff_revenue_profit(date_old)
    
    aff_merged = pd.merge(
        aff_new.rename(columns={"Total Revenue": "最新 Revenue", "Total Profit": "最新 Profit"}),
        aff_old.rename(columns={"Total Revenue": "次新 Revenue", "Total Profit": "次新 Profit"}),
        on="Affiliate",
        how="outer"
    ).fillna(0)
    
    # 计算利润率和变化幅度
    aff_merged["最新 利润率"] = aff_merged["最新 Profit"] / aff_merged["最新 Revenue"].replace(0, np.nan)
    aff_merged["次新 利润率"] = aff_merged["次新 Profit"] / aff_merged["次新 Revenue"].replace(0, np.nan)
    aff_merged["Revenue 变化幅度(%)"] = (aff_merged["最新 Revenue"] - aff_merged["次新 Revenue"]) / aff_merged["次新 Revenue"].replace(0, np.nan) 
    aff_merged["Profit 变化幅度(%)"] = (aff_merged["最新 Profit"] - aff_merged["次新 Profit"]) / aff_merged["次新 Profit"].replace(0, np.nan) 
    aff_merged["利润率 变化幅度(%)"] = (aff_merged["最新 利润率"] - aff_merged["次新 利润率"]) / aff_merged["次新 利润率"].replace(0, np.nan) 
    
    
    
     # 步骤b：计算reject率
    def calculate_aff_reject_count(date, df):
        df_filtered = df[(df["Time"] == date) & (df["是否为reject"] == True)]
        return df_filtered.groupby("Affiliate").agg({
        "是否为reject": "count" }).rename(columns={"是否为reject": "Total reject"})
    
    aff_reject_new = calculate_aff_reject_count(date_new, reject_event_df)
    aff_reject_old = calculate_aff_reject_count(date_old, reject_event_df)
    
    def calculate_aff_conversions(date):
        return flow_df[flow_df["Time"] == date].groupby("Affiliate").agg({
            "Total Conversions": "sum"
        }).reset_index()
    
    aff_conv_new = calculate_aff_conversions(date_new)
    aff_conv_old = calculate_aff_conversions(date_old)
    
    # 合并reject和conversions数据
    aff_reject_new = pd.merge(aff_reject_new, aff_conv_new, on="Affiliate", how="outer").fillna(0)
    aff_reject_old = pd.merge(aff_reject_old, aff_conv_old, on="Affiliate", how="outer").fillna(0)
    
    aff_reject_new["reject率"] = aff_reject_new["Total reject"] / (aff_reject_new["Total reject"] + aff_reject_new["Total Conversions"]).replace(0, np.nan)
    aff_reject_old["reject率"] = aff_reject_old["Total reject"] / (aff_reject_old["Total reject"] + aff_reject_old["Total Conversions"]).replace(0, np.nan)
    
    # 最终合并所有数据
    final_aff = pd.merge(aff_merged, aff_reject_new[["Affiliate", "Total reject", "reject率"]].rename(columns={"Total reject": "最新 Total reject", "reject率": "最新 reject率"}), on="Affiliate", how="outer")
    final_aff = pd.merge(final_aff, aff_reject_old[["Affiliate", "Total reject", "reject率"]].rename(columns={"Total reject": "次新 Total reject", "reject率": "次新 reject率"}), on="Affiliate", how="outer")
    
    final_aff.rename(columns={'最新 Revenue': f'{date_new} Total Revenue'}, inplace=True)
    final_aff.rename(columns={'最新 Profit': f'{date_new} Total Profit'}, inplace=True)
    final_aff.rename(columns={'次新 Revenue': f'{date_old} Total Revenue'}, inplace=True)
    final_aff.rename(columns={'次新 Profit': f'{date_old} Total Profit'}, inplace=True)
    final_aff.rename(columns={'最新 利润率': f'{date_new} 利润率'}, inplace=True)
    final_aff.rename(columns={'次新 利润率': f'{date_old} 利润率'}, inplace=True)
    final_aff.rename(columns={'最新 Total reject': f'{date_new} Total reject'}, inplace=True)
    final_aff.rename(columns={'最新 reject率': f'{date_new} reject率'}, inplace=True)
    final_aff.rename(columns={'次新 Total reject': f'{date_old} Total reject'}, inplace=True)
    final_aff.rename(columns={'次新 reject率': f'{date_old} reject率'}, inplace=True)
    
    
    return final_aff.fillna(0)


def calculate_large_drop_budget(sheets,offer_base_info):
    """
    规则6：计算上周四到今天利润下降幅度较大的预算
    参数：
        sheets: 包含【1--过去30天总流水】的字典（key为sheet名，value为DataFrame）
    返回：
        result_df: 包含所有要求字段的利润下降预算分析结果
    """
    # ======================
    # 1. 数据预处理
    # ======================
    df = sheets["流水数据"].copy()
    target_col = 'Total Caps'

    # 步骤1：尝试转换为数值类型，无法转换的变为NaN
    df[target_col] = pd.to_numeric(df[target_col], errors='coerce')

    # 步骤2：筛选条件：非数字(NaN) 或 数值≤0
    condition = (df[target_col].isna()) | (df[target_col] <= 0)
    # 统一列名映射
    rename_map = {
        "Offer ID": "offerid",
        "Adv Offer ID": "adv_offer_id",
        "Advertiser": "advertiser",
        "App ID": "appid",
        "GEO": "country",
        "Total Caps": "total_cap",
        "Total Clicks": "clicks",
        "Total Conversions": "conversions",
        "Total Revenue": "revenue",
        "Total Profit": "profit",
        "Online hour": "online_hour",
        "Status": "status",
        "Affiliate": "affiliate",
        "Time": "date"
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    
    
    # 日期处理：转为date格式，提取关键时间节点
    today = datetime.now().date()  # 今天
    monday_of_this_week = today - timedelta(days=today.weekday())
    last_thursday = monday_of_this_week - timedelta(days=4)

    latest_date = df["date"].max()  # 数据中最新一天
    penultimate_date = sorted(df["date"].unique())[-2] if len(df["date"].unique()) >=2 else latest_date  # 次次新一天
    
    # 筛选时间范围：上周四到次次新
    time_range = (df["date"] >= last_thursday) & (df["date"] < penultimate_date)
    df_time_filtered = df[time_range].copy()
    
    # 数值字段兜底空值为0
    num_cols = ["clicks", "conversions", "revenue", "profit", "online_hour"]
    df[num_cols] = df[num_cols].fillna(0).astype(float)
    df_time_filtered[num_cols] = df_time_filtered[num_cols].fillna(0).astype(float)
    
    # ======================
    # 2. 找到每个offerid的历史最高利润日（上周四到次次新）
    # ======================
    # 按offerid+date聚合日度数据
    offer_daily = df_time_filtered.groupby(["offerid", "date"]).agg({
        "profit": "sum",
        "revenue": "sum",
        "online_hour": "max",
        "status": "first",
        "total_cap": "first",
        "adv_offer_id": "first",
        "advertiser": "first",
        "appid": "first",
        "country": "first"
    }).reset_index()
    
    # 找到每个offerid的历史最高利润日
    offer_max_profit = offer_daily.loc[offer_daily.groupby("offerid")["profit"].idxmax()].copy()
    offer_max_profit = offer_max_profit.rename(columns={
        "profit": "max_profit",
        "revenue": "max_revenue",
        "online_hour": "max_online_hour",
        "date": "max_profit_date",
        "status": "max_status",
        "total_cap": "max_total_cap"
    })
    
    # ======================
    # 3. 提取每个offerid最新一天的数据
    # ======================
    latest_data = df[df["date"] == latest_date].groupby("offerid").agg({
        "profit": "sum",
        "revenue": "sum",
        "online_hour": "max",
        "status": "first",
        "total_cap": "first",
        "adv_offer_id": "first",
        "advertiser": "first",
        "appid": "first",
        "country": "first"
    }).reset_index()
    latest_data = latest_data.rename(columns={
        "profit": "latest_profit",
        "revenue": "latest_revenue",
        "online_hour": "latest_online_hour",
        "status": "latest_status",
        "total_cap": "latest_total_cap"
    })
    
    # ======================
    # 4. 合并数据，筛选利润下降的offerid（核心修复：新增历史有利润/最新无利润的条件）
    # ======================
    offer_merge = pd.merge(
        offer_max_profit[["offerid", "max_profit", "max_revenue", "max_online_hour", "max_profit_date", "max_status"]],
        latest_data,
        on="offerid",
        how="outer"  # 关键修改：inner -> outer
    )
    
    # 可选：填充缺失值（推荐，避免后续分析出现NaN问题）
    # 数值型字段填充为0，字符串/状态字段填充为特定标识
    offer_merge = offer_merge.fillna({
        "max_profit": 0,
        "max_revenue": 0,
        "max_online_hour": 0,
        "latest_profit": 0,
        "latest_revenue": 0,
        "latest_online_hour": 0,
        "max_status": "未知",
        "latest_status": "未知",
        "adv_offer_id": "",
        "advertiser": "",
        "appid": "",
        "country": ""
    })
    
    # 计算利润差值
    offer_merge["profit_diff"] = offer_merge["latest_profit"] - offer_merge["max_profit"]
    
    # 核心筛选条件（修复后）：
    # 条件1：利润差值 ≤ -10 美金（原有）
    condition1 = offer_merge["profit_diff"] <= -5.0
   
    drop_offers = offer_merge[condition1].copy()
    
    # 对条件2的offerid，重新计算profit_diff（确保文本逻辑兼容）
 
    
 
    
    if drop_offers.empty:
        return pd.DataFrame()
    
    # ======================
    # 工具函数
    # ======================
    def format_num(x):
        """数值保留2位小数"""
        return round(float(x), 2)
    
    def format_pct(x):
        """百分比保留1位小数"""
        return f"{round(float(x) * 100, 1)}%"
    
    def safe_div(a, b):
        """安全除法，避免除以0"""
        a = float(a)
        b = float(b)
        return a / b if b != 0 else 0
    
    def pct_change(new, old):
        """计算变化百分比"""
        new = float(new)
        old = float(old)
        return (new - old) / old * 100 if old != 0 else 0

    offer_base_info.rename(columns={'Offer Id': 'offerid'}, inplace=True)

    offer_base_info['offerid'] = offer_base_info['offerid'].astype(int)
    
    drop_offers = drop_offers.merge(offer_base_info[['offerid','Adv Offer ID','App ID','Advertiser', "GEO",'Total Caps', 'Status', 'Payin']],
        on = 'offerid',
        how='left')


    
    target_col = 'Total Caps'

    # 步骤1：尝试转换为数值类型，无法转换的变为NaN
    drop_offers[target_col] = pd.to_numeric(drop_offers[target_col], errors='coerce')

    # 步骤2：筛选条件：非数字(NaN) 或 数值≤0
    condition = (drop_offers[target_col].isna()) | (drop_offers[target_col] <= 0)
    
    drop_offers.loc[condition, target_col] = 100   
    
    
    # ======================
    # 5. 遍历筛选后的offerid，处理Affiliate维度
    # ======================
    rows = []

    for _, offer_row in drop_offers.iterrows():

        
        
        offer_id = offer_row["offerid"]
        
        max_profit_date = offer_row["max_profit_date"]# 历史最高利润日
        latest_date_val = latest_date  # 最新一天
        profit_diff = offer_row["profit_diff"]  # 最新-历史最高 利润差值
        
        # 提取该offerid的基础信息
        adv_offer_id = offer_row["Adv Offer ID"]
        advertiser = offer_row["Advertiser"]
        appid = offer_row["App ID"]
        country = offer_row["GEO"]
        latest_total_cap = offer_row["Total Caps"]
        latest_status = offer_row["Status"]
        est_price = offer_row["Payin"]
        
        # 提取该offerid在历史最高利润日和最新一天的全量数据
        df_offer = df[df["offerid"] == offer_id].copy()
        df_offer_max = df_offer[df_offer["date"] == max_profit_date].copy()
        df_offer_latest = df_offer[df_offer["date"] == latest_date_val].copy()
        
        # ======================
        # 5.1 计算基础指标
        # ======================
        # 历史最高利润日指标
        max_profit = format_num(offer_row["max_profit"])
        max_revenue = format_num(offer_row["max_revenue"])
        max_online_hour = format_num(offer_row["max_online_hour"])
        max_margin = format_pct(safe_div(offer_row["max_profit"], offer_row["max_revenue"]))
        
        # 最新一天指标
        latest_profit = format_num(offer_row["latest_profit"])
        latest_revenue = format_num(offer_row["latest_revenue"])
        latest_online_hour = format_num(offer_row["latest_online_hour"])
        latest_margin = format_pct(safe_div(offer_row["latest_profit"], offer_row["latest_revenue"]))
        

        
        # 在线时长差值
        oh_diff = format_num(float(latest_online_hour) - float(max_online_hour))
        
        # ======================
        # 5.2 计算Affiliate维度数据
        # ======================
        # 生成Affiliate全量网格（历史最高日+最新日）
        all_affiliates = df_offer["affiliate"].unique().tolist() if not df_offer.empty else ["未知Affiliate"]
        if not all_affiliates:
            all_affiliates = ["未知Affiliate"]
        
        aff_date_grid = pd.MultiIndex.from_product(
            [all_affiliates, [max_profit_date, latest_date_val]],
            names=["affiliate", "date"]
        ).to_frame(index=False)
        
        # 按Affiliate+date聚合
        aff_daily = df_offer.groupby(["affiliate", "date"]).agg({
            "clicks": "sum",
            "conversions": "sum",
            "revenue": "sum",
            "profit": "sum",
            "online_hour": "max"
        }).reset_index()
        
        # 合并网格，无数据填充0
        aff_full = pd.merge(aff_date_grid, aff_daily, on=["affiliate", "date"], how="left").fillna(0.0)
        
        # 拆分历史最高日和最新日数据
        aff_max = aff_full[aff_full["date"] == max_profit_date].copy().rename(
            columns={col: f"{col}_max" for col in ["clicks", "conversions", "revenue", "profit", "online_hour"]}
        )
        aff_latest = aff_full[aff_full["date"] == latest_date_val].copy().rename(
            columns={col: f"{col}_latest" for col in ["clicks", "conversions", "revenue", "profit", "online_hour"]}
        )
        
        # 合并Affiliate数据
        aff_merge = pd.merge(
            aff_max[["affiliate", "clicks_max", "conversions_max", "revenue_max", "profit_max"]],
            aff_latest[["affiliate", "clicks_latest", "conversions_latest", "revenue_latest", "profit_latest"]],
            on="affiliate",
            how="outer"
        ).fillna(0.0)
        
        # 计算CR、利润率、变化值
        aff_merge["cr_max"] = aff_merge.apply(lambda x: safe_div(x["conversions_max"], x["clicks_max"]), axis=1)
        aff_merge["cr_latest"] = aff_merge.apply(lambda x: safe_div(x["conversions_latest"], x["clicks_latest"]), axis=1)
        aff_merge["margin_max"] = aff_merge.apply(lambda x: safe_div(x["profit_max"], x["revenue_max"]), axis=1)
        aff_merge["margin_latest"] = aff_merge.apply(lambda x: safe_div(x["profit_latest"], x["revenue_latest"]), axis=1)
        aff_merge["profit_change"] = aff_merge["profit_latest"] - aff_merge["profit_max"]
        aff_merge["revenue_change"] = aff_merge["revenue_latest"] - aff_merge["revenue_max"]
        
        # 计算变化百分比
        aff_merge["revenue_pct"] = aff_merge.apply(lambda x: pct_change(x["revenue_latest"], x["revenue_max"]), axis=1)
        aff_merge["clicks_pct"] = aff_merge.apply(lambda x: pct_change(x["clicks_latest"], x["clicks_max"]), axis=1)
        aff_merge["cr_pct"] = aff_merge.apply(lambda x: pct_change(x["cr_latest"], x["cr_max"]), axis=1)
        aff_merge["margin_pct"] = aff_merge.apply(lambda x: pct_change(x["margin_latest"], x["margin_max"]), axis=1)
        
        # ======================
        # 5.3 筛选影响的Affiliate（利润变化≤-3美金）
        # ======================
        aff_affect = aff_merge[
            (aff_merge["profit_change"] <= -3.0) 
        ].copy()
        
        # 生成下游影响文本
        downstream_text = []
        for _, arow in aff_affect.iterrows():
            aff_name = arow["affiliate"]
            apc = format_num(arow["profit_change"])  # Affiliate利润变化
            p_max = format_num(arow["profit_max"])    # 历史最高利润日Profit
            p_latest = format_num(arow["profit_latest"])  # 最新一天Profit
            r_max = format_num(arow["revenue_max"])   # 历史最高利润日Revenue
            r_latest = format_num(arow["revenue_latest"]) # 最新一天Revenue
            c_max = format_num(arow["clicks_max"])    # 历史最高利润日Clicks
            c_latest = format_num(arow["clicks_latest"])  # 最新一天Clicks
            cr_max = format_pct(arow["cr_max"])       # 历史最高利润日CR
            cr_latest = format_pct(arow["cr_latest"]) # 最新一天CR
            m_max = format_pct(arow["margin_max"])    # 历史最高利润日利润率
            m_latest = format_pct(arow["margin_latest"])  # 最新一天利润率
            rp = f"{round(arow['revenue_pct'], 1)}%"  # Revenue变化%
            cp = f"{round(arow['clicks_pct'], 1)}%"   # Clicks变化%
            crp = f"{round(arow['cr_pct'], 1)}%"      # CR变化%
            
            # 子场景1：最新一天Profit减少为0（重点兼容新增筛选的场景）
            if float(p_latest) == 0 and float(p_max) != 0:
                reduce_revenue = format_num(float(r_max) - float(r_latest))
                txt = (f"{aff_name} 停止产生流水，减少流水 {reduce_revenue} 美金，"
                       f"对应Total revenue从 {r_max} 美金（{max_profit_date}）变为 {r_latest} 美金（{latest_date_val}）")
            
            # 子场景2：Profit未减少为0（≤-3美金）
            else:
                txt = (f"{aff_name} 的Total Profit影响 {apc} 美金，"
                       f"对应Total Profit从 {p_max} 美金（{max_profit_date}）变为 {p_latest} 美金（{latest_date_val}）")
                
                # 拆解影响因素：流水贡献 vs 利润率贡献
                rev_contrib = (float(r_latest) - float(r_max)) * safe_div(arow["profit_max"], r_max) if float(r_max) != 0 else 0.0
                margin_contrib = float(r_latest) * (safe_div(arow["profit_latest"], r_latest) - safe_div(arow["profit_max"], r_max)) if float(r_latest) != 0 else 0.0
                rev_contrib = format_num(rev_contrib)
                margin_contrib = format_num(margin_contrib)
                total_contrib = abs(float(rev_contrib)) + abs(float(margin_contrib))

                if total_contrib < 1e-6:
                    factor_txt = ""
                else:
                    rev_ratio = abs(float(rev_contrib)) / total_contrib  # 流水影响占比
                    margin_ratio = abs(float(margin_contrib)) / total_contrib  # 利润率影响占比

                    # 流水影响超80%
                    if rev_ratio > 0.8:
                        factor_txt = (f"，主要受流水下降影响，影响利润 {rev_contrib} 美金，"
                                     f"Total revenue从 {r_max} 美金变为 {r_latest} 美金，变化{rp}，"
                                     f"Total Clicks从 {c_max} 变为 {c_latest}，变化{cp}，"
                                     f"CR从 {cr_max} 变为 {cr_latest}，变化{crp}")
                    # 利润率影响超80%
                    elif margin_ratio > 0.8:
                        factor_txt = (f"，主要受利润率下降影响，影响利润 {margin_contrib} 美金，"
                                     f"利润率从 {m_max} 变为 {m_latest}，"
                                     f"请检查是否价格/预算设置发生改变，导致利润率下降")
                    # 二者共同影响
                    else:
                        factor_txt = (f"，流水和利润率分别影响 {rev_contrib} 美金和 {margin_contrib} 美金，"
                                     f"Total revenue从 {r_max} 美金变为 {r_latest} 美金，变化{rp}，"
                                     f"Total Clicks从 {c_max} 变为 {c_latest}，变化{cp}，"
                                     f"CR从 {cr_max} 变为 {cr_latest}，变化{crp}，"
                                     f"同时利润率从 {m_max} 变为 {m_latest}，"
                                     f"请检查是否价格/预算设置发生改变，导致利润率发生变化")
                txt += factor_txt
            downstream_text.append(txt)
        
        # 处理下游文本：无变化/多Affiliate分隔（; + 换行）
        if not downstream_text:
            downstream_final = "无下游有明显变化"
        else:
            # 用;分隔，同时添加换行符（Excel单元格内换行）
            downstream_final = "; \n".join(downstream_text)
        
        # ======================
        # 5.4 生成在线时长和预算状态总结
        # ======================
        if latest_status == "PAUSE":
            status_summary = "预算已暂停，优先询问广告主预算暂停原因"
            print(1,)
        elif latest_status == "ACTIVE":
            oh_diff_float = float(latest_online_hour) - float(max_online_hour)
            # 兼容新增场景：历史有利润/最新无利润
            if (oh_diff_float >= 0 and profit_diff <= -10.0) :
                print(2,)
                status_summary = (f"在线时长无变化（{max_profit_date}：{max_online_hour}小时 → {latest_date_val}：{latest_online_hour}小时），"
                                 f"但利润有明显下降，重点沟通影响下游")
            elif oh_diff_float < -4 and profit_diff <= -10.0:
                print(3,)
                status_summary = (f"在线时长减少4小时以上（{max_profit_date}：{max_online_hour}小时 → {latest_date_val}：{latest_online_hour}小时），"
                                 f"先和广告主沟通预算是否不足，因为预算在线时长较短")
            else:
                status_summary = ""
                print(4,)
        else:
            status_summary = ""
            print(5,)
        
        # ======================
        # 5.5 标记新/旧预算（近7天首次产生流水）
        # ======================
        if not df_offer.empty:
            first_revenue_date = df_offer[df_offer["revenue"] > 0]["date"].min()
            is_new_budget = first_revenue_date >= (latest_date_val - timedelta(days=7))
        else:
            is_new_budget = False
        budget_type = "新预算" if is_new_budget else "旧预算"
        
        # ======================
        # 5.6 组装结果行
        # ======================
        rows.append({
            "offer id": offer_id,
            "adv offer id": adv_offer_id,
            "Advertiser": advertiser,
            "appid": appid,
            "country": country,
            "昨日Total cap": format_num(latest_total_cap),
            "Payin": est_price,
            "昨日online hour（小时）": latest_online_hour,
            "历史最高利润对应日期":max_profit_date,
            "历史最高利润当天online hour（小时）": max_online_hour,
            "昨日Total revenue（美金）": latest_revenue,
            "历史最高利润当天Total revenue（美金）": max_revenue,
            "昨日Total profit（美金）": latest_profit,
            "历史最高利润当天profit一天Total profit（美金）": max_profit,
            "昨日利润率": latest_margin,
            "历史最高利润当天利润率": max_margin,
            "Total profit变化差值（美金）": format_num(profit_diff),
            "online hour变化差值（小时）": oh_diff,
            "预算status状态": latest_status,
            "在线时长和预算状态总结": status_summary,
            "具体影响下游总结": downstream_final,
            "预算类型": budget_type
        })
        
        


    
    
    # ======================
    # 6. 结果格式化输出
    # ======================
    result_df = pd.DataFrame(rows)
    # 确保数值列类型正确
    for col in result_df.columns:
        if "%" in col or "总结" in col or "类型" in col or "状态" in col or "offer id" in col:
            continue
        result_df[col] = pd.to_numeric(result_df[col], errors="ignore")
    
    return result_df


def calculate_profit_influence(sheets, date_new, date_old):
    """规则7：计算利润影响因素（最终优化版）
    新增核心逻辑：利润变化绝对值百分比<5% → 视为稳定，不执行offer+affiliate深度分析
    """
    flow_df = sheets["流水数据"].copy()
    # 确保关键字段类型正确

    flow_df["Offer ID"] = flow_df["Offer ID"].astype(str)  # 统一offer_id类型
    flow_df["Affiliate"] = flow_df["Affiliate"].fillna("未知Affiliate")  # 兜底空值
    
    # 格式化日期字符串
    date_new_str = date_new
    date_old_str = date_old
    
    # ---------------------- 1. 全局利润/流水/利润率计算 ----------------------
    # 筛选最近两天数据
    flow_recent = flow_df[flow_df["Time"].isin([date_new, date_old])].copy()
    
    # 全局汇总（最近两天）
    total_summary = flow_recent.groupby("Time").agg({
        "Total Revenue": "sum",
        "Total Profit": "sum"
    }).reset_index()
    
    # 拆分新/旧数据（兜底空数据）
    total_new = total_summary[total_summary["Time"] == date_new].iloc[0] if not total_summary[total_summary["Time"] == date_new].empty else pd.Series([0, 0], index=["Total Revenue", "Total Profit"])
    total_old = total_summary[total_summary["Time"] == date_old].iloc[0] if not total_summary[total_summary["Time"] == date_old].empty else pd.Series([0, 0], index=["Total Revenue", "Total Profit"])
    
    # 基础指标（全局）
    rev_new = total_new["Total Revenue"]
    rev_old = total_old["Total Revenue"]
    profit_new = total_new["Total Profit"]
    profit_old = total_old["Total Profit"]
    
    # 全局利润率（避免除以0）
    profit_margin_new = profit_new / rev_new if rev_new != 0 else np.nan
    profit_margin_old = profit_old / rev_old if rev_old != 0 else np.nan
    
    # 环比计算（全局）
    rev_abs_change = rev_new - rev_old  # 流水绝对值变化
    rev_pct_change = (rev_abs_change / rev_old) * 100 if rev_old != 0 else np.nan
    profit_abs_change = profit_new - profit_old  # 利润绝对值变化
    profit_pct_change = (profit_abs_change / profit_old) * 100 if profit_old != 0 else np.nan
    margin_abs_change = profit_margin_new - profit_margin_old  # 利润率绝对值变化
    margin_pct_change = (margin_abs_change / profit_margin_old) * 100 if profit_margin_old != 0 else np.nan
    
    # ---------------------- 2. 核心判断：利润波动是否≥5%（新增关键逻辑） ----------------------
    # 计算利润变化绝对值百分比（兜底NaN情况）
    profit_fluctuation_pct = abs(profit_pct_change) if not np.isnan(profit_pct_change) else 0
    # 判定是否稳定：<5% → 稳定，≥5% → 需分析
    is_profit_stable = profit_fluctuation_pct < 5.0
    
    # ---------------------- 3. 仅当利润不稳定时，计算全局贡献度+驱动因素 ----------------------
    influence_type = ""
    factor_text = ""
    revenue_contribution = 0
    margin_contribution = 0
    total_contribution = 0
    profit_trend = "持平"
    
    if not is_profit_stable:
        # 全局贡献度（流水/利润率对利润变化的影响）
        revenue_contribution = rev_abs_change * profit_margin_old if not np.isnan(profit_margin_old) else 0
        margin_contribution = rev_new * margin_abs_change if not np.isnan(margin_abs_change) else 0
        total_contribution = revenue_contribution + margin_contribution
        
        # 核心驱动因素（流水/利润率/共同）
        if abs(total_contribution) < 1e-6:  # 利润无变化（兜底）
            influence_type = "无"
            factor_text = "无明显因素"
        elif abs(revenue_contribution) / abs(total_contribution) > 0.8:
            influence_type = "流水"
            factor_text = "流水变化"
        elif abs(margin_contribution) / abs(total_contribution) > 0.8:
            influence_type = "利润率"
            factor_text = "利润率变化"
        else:
            influence_type = "共同"
            factor_text = "流水变化和利润率变化"
        
        # 利润涨跌方向
        profit_trend = "上涨" if profit_abs_change > 0 else "下降" if profit_abs_change < 0 else "持平"
    
    # ---------------------- 4. 仅当利润不稳定时，执行offer+affiliate深度分析 ----------------------
    offer_analysis_result = []
    if not is_profit_stable and profit_trend != "持平" and influence_type != "无":
        # 3.1 先按offer_id+Time聚合（基础信息+整体指标）
        offer_static = flow_recent.groupby("Offer ID").agg({
        "Advertiser": "first",  # 一个Offer只有一个Advertiser，直接取第一个
        "Adv Offer ID": "first",
        "App ID": "first",
        "GEO": "first"
       }).reset_index()  # 重置索引，方便后续合并
        
        offer_dynamic = flow_recent.groupby(["Offer ID", "Time"]).agg({
        "Total Revenue": "sum",
        "Total Profit": "sum"}).unstack().fillna(0)  # 只对动态指标unstack
        
        offer_dynamic.columns = [f"{col[0]}_{col[1].strftime('%Y-%m-%d')}" for col in offer_dynamic.columns]
        offer_dynamic = offer_dynamic.reset_index()  # 重置索引，Offer ID变为列
        
    
        offer_base = pd.merge(
        offer_static,
        offer_dynamic,
        on="Offer ID",
        how="inner" )  # 只保留有动态指标的Offer
  
    
        
        # 整理列名（统一新/旧命名）
        cols = offer_base.columns
        date_cols = [date_old_str, date_new_str] 
        
        offer_base.rename(columns={
            f"Total Revenue_{date_old_str}": "old_Revenue",
            f"Total Revenue_{date_new_str}": "new_Revenue",
            f"Total Profit_{date_old_str}": "old_Profit",
            f"Total Profit_{date_new_str}": "new_Profit"
        }, inplace=True)
        
        # 3.2 计算offer级核心指标
        offer_base["offer_profit_change"] = offer_base["new_Profit"] - offer_base["old_Profit"]  # offer总利润变化
        offer_base["old_margin"] = offer_base["old_Profit"] / offer_base["old_Revenue"].replace(0, np.nan)  # offer旧利润率
        offer_base["new_margin"] = offer_base["new_Profit"] / offer_base["new_Revenue"].replace(0, np.nan)  # offer新利润率
        # offer级流水/利润率分项影响
        offer_base["revenue_driven_change"] = (offer_base["new_Revenue"] - offer_base["old_Revenue"]) * offer_base["old_margin"]
        offer_base["margin_driven_change"] = offer_base["new_Revenue"] * (offer_base["new_margin"] - offer_base["old_margin"])
        offer_base[["revenue_driven_change", "margin_driven_change"]] = offer_base[["revenue_driven_change", "margin_driven_change"]].fillna(0)
        
        # 3.3 按利润涨跌方向排序（offer级）
        if profit_trend == "下降":
            offer_sorted = offer_base.sort_values("offer_profit_change", ascending=True)  # 下降升序
            sort_desc = "升序"
        else:
            offer_sorted = offer_base.sort_values("offer_profit_change", ascending=False)  # 上涨降序
            sort_desc = "降序"
        
        # 3.4 筛选利润绝对值变化超过10美金的核心offer
        total_offer_change = offer_sorted["offer_profit_change"].sum()
        if abs(total_offer_change) < 1e-6:
            top_offers = pd.DataFrame()
        else:
            offer_sorted["cumulative_change"] = offer_sorted["offer_profit_change"].cumsum()
            offer_sorted["cumulative_ratio"] = (offer_sorted["cumulative_change"] / total_offer_change * 100)
            #top_offers = offer_sorted[offer_sorted["cumulative_ratio"] <= 80.0].copy()
            if profit_trend == "下降":
                top_offers = offer_sorted[offer_sorted["revenue_driven_change"] <- 10.0].copy()
            else:
                top_offers = offer_sorted[offer_sorted["revenue_driven_change"] >= 10.0].copy()
            # 兜底：无满足条件时取累计最接近的前10个
            if top_offers.empty:
                offer_sorted["cumulative_ratio"] = offer_sorted["cumulative_change"].abs().cumsum() / abs(total_offer_change) * 100
                top_offers = offer_sorted[offer_sorted["cumulative_ratio"] <= 80.0].head(10)
        
        # 3.5 拆解核心offer的affiliate维度影响
        if not top_offers.empty:
            core_offer_ids = top_offers['Offer ID'].tolist()  # 核心offer列表
            # 筛选核心offer的affiliate数据
            aff_data = flow_recent[flow_recent["Offer ID"].isin(core_offer_ids)].copy()
            
            # 按offer_id+Affiliate+Time聚合affiliate级数据
            aff_base = aff_data.groupby(["Offer ID", "Affiliate", "Time"]).agg({
                "Total Profit": "sum"
            }).unstack().fillna(0)
            aff_base.columns = [f"{date_old_str}_Profit", f"{date_new_str}_Profit"] if date_old in aff_base.columns.levels[1] else [f"{date_new_str}_Profit", f"{date_old_str}_Profit"]
            aff_base.rename(columns={
                f"{date_old_str}_Profit": "old_aff_Profit",
                f"{date_new_str}_Profit": "new_aff_Profit"
            }, inplace=True)
            # 计算每个affiliate的利润变化
            aff_base["aff_profit_change"] = aff_base["new_aff_Profit"] - aff_base["old_aff_Profit"]
            aff_base.reset_index(inplace=True)
            
            # 整理每个核心offer的信息（含affiliate拆解）
            for offer_id in core_offer_ids:
                offer_row = top_offers[top_offers['Offer ID']==offer_id]
                # 提取该offer的基础信息
                offer_info = {
                    "offer_id": offer_id,
                    "advertiser": offer_row["Advertiser"],
                    "adv_offerid": offer_row["Adv Offer ID"],
                    "appid": offer_row["App ID"],
                    "geo": offer_row["GEO"],
                    "offer_profit_change": offer_row["offer_profit_change"],
                    "revenue_driven": offer_row["revenue_driven_change"],
                    "margin_driven": offer_row["margin_driven_change"],
                    "affiliates": []  # 存储该offer下的affiliate影响
                }
                
                # 提取该offer下的affiliate数据
                offer_aff_data = aff_base[aff_base["Offer ID"] == offer_id]
                if not offer_aff_data.empty:
                    for _, aff_row in offer_aff_data.iterrows():
                        offer_info["affiliates"].append({
                            "affiliate": aff_row["Affiliate"],
                            "aff_profit_change": aff_row["aff_profit_change"]
                        })
                
                offer_analysis_result.append(offer_info)
    
    # ---------------------- 5. 生成最终结论文本 ----------------------
    # 基础结论
    base_conclusion = (
        f"昨日流水(Total revenue){rev_new:.2f}美金（{date_new_str}），环比{date_old_str}变化{rev_pct_change:.1f}%（绝对值变化{rev_abs_change:.2f}美金），"
        f"利润{profit_new:.2f}美金，环比{date_old_str}变化{profit_pct_change:.1f}%（绝对值变化{profit_abs_change:.2f}美金），"
        f"利润率(Total profit/Total revenue){profit_margin_new:.4f}，环比{date_old_str}变化{margin_pct_change:.1f}%（绝对值变化{margin_abs_change:.4f}）"
    )
    
    # 分场景生成结论
    if is_profit_stable:
        # 场景1：利润稳定（波动<5%）→ 仅输出稳定结论
        final_conclusion = f"{base_conclusion}，利润变化幅度{profit_fluctuation_pct:.1f}%<5%，利润整体稳定，无需进一步分析。"
    else:
        # 场景2：利润不稳定（波动≥5%）→ 输出完整分析
        # 核心驱动因素结论
        driver_conclusion = (
            f"；其中利润变化受流水变化影响{revenue_contribution:.2f}美金，受利润率变化影响{margin_contribution:.2f}美金；"
            f"近两日（{date_old_str}至{date_new_str}）利润{profit_trend}，主要由{factor_text}驱动"
        )
        
        # offer+affiliate维度分析结论
        offer_conclusion = ""
        if offer_analysis_result:
            offer_texts = []
            for offer in offer_analysis_result:

                # 拼接offer级信息
                offer_text = (
                    f"Offer ID：{offer['offer_id']}（广告主：{offer['advertiser'].values[0]}，Adv Offer ID：{offer['adv_offerid'].values[0]}，App ID：{offer['appid'].values[0]}，GEO：{offer['geo'].values[0]}），"
                    f"影响利润{offer['offer_profit_change'].values[0]:.2f}美金（流水影响{offer['revenue_driven'].values[0]:.2f}美金，利润率影响{offer['margin_driven'].values[0]:.2f}美金）"
                )
                # 拼接affiliate级信息
                if offer["affiliates"]:
                    aff_texts = []
                    for aff in offer["affiliates"]:
                        aff_texts.append(f"{aff['affiliate']}（影响利润{aff['aff_profit_change']:.2f}美金）")
                    offer_text += f"；该Offer下核心Affiliate影响：{'; '.join(aff_texts)}"
                offer_texts.append(offer_text)
            
            offer_conclusion = f"；累计贡献利润{profit_trend}幅度≥80%的核心Offer如下（按{sort_desc}排序）：{'; '.join(offer_texts)}"
        else:
            offer_conclusion = "；未找到累计贡献利润变化≥80%的核心Offer"
        
        # 拼接最终结论
        final_conclusion = base_conclusion + driver_conclusion + offer_conclusion + "。"
    
    return final_conclusion



def calculate_event_analysis(sheets,offer_base_info):
    """计算event事件分析（单独输出Excel）"""
    event_df = sheets["event事件"].copy()
    reject_rule_df = sheets["reject规则"].copy()
    adv_match_df = sheets["广告主匹配"].copy()
    flow_df = sheets["流水数据"].copy()
    

    
    # 预处理：去除Event为空的数据
    event_df = event_df.dropna(subset=["Event"])
    
    # 匹配是否为reject和广告主信息
    event_df = pd.merge(event_df, reject_rule_df[["Event", "是否为reject"]], on="Event", how="left").fillna({"是否为reject": False})
    event_df = pd.merge(
        event_df,
        adv_match_df[["Advertiser", "二级广告主", "三级广告主"]],
        on="Advertiser",
        how="left"
    )
    
    # 调整Appnext的Time字段（同步修正此处的判断逻辑）
    event_df.loc[(event_df["是否为reject"] == True) & (event_df["三级广告主"] == "Appnext"), "Time"] -= timedelta(days=1)
    
    # 提取Offer Id（从Offer Name的【xx】中提取数字）
    def extract_offer_id(offer_name):
        match = re.search(r"\[(\d+)\]", str(offer_name))
        return match.group(1) if match else ""
    
    event_df["Offer Id"] = event_df["Offer Name"].apply(extract_offer_id)
    
    # 1、计算event--reject事件
    reject_event = event_df[event_df["是否为reject"] == True].copy()
    
    # 步骤f：计算总体reject rate


    flow_conv = flow_df.groupby(["Time", "Offer ID", "Advertiser", "App ID", "GEO"]).agg({
        "Total Conversions": "sum"
    }).reset_index().rename(columns={"Offer ID": "Offer Id"})
    
    
    
    reject_total = reject_event.groupby(["Time", "Offer Id"]).agg({
        "是否为reject": "sum"
    }).reset_index().rename(columns={"是否为reject": "Total reject"})
    
    reject_total_copy = reject_total.copy()
    flow_conv_copy = flow_conv.copy()
    
    # 2. 统一"Offer Id"字段为字符串类型（兼容所有格式，避免类型冲突）
    # 处理可能的空值，填充为"未知Offer"后转字符串
    reject_total_copy["Offer Id"] = reject_total_copy["Offer Id"].fillna("未知Offer").astype(str)
    flow_conv_copy["Offer Id"] = flow_conv_copy["Offer Id"].fillna("未知Offer").astype(str)
    

        
    reject_rate_total = pd.merge(
        reject_total_copy,
        flow_conv_copy,
        on=["Time", "Offer Id"],  # 现在字段类型完全一致，可正常匹配
        how="left"
    ).fillna(0)
    
    reject_total['Offer Id']=reject_total['Offer Id'].astype(str)
    flow_conv['Offer Id']=flow_conv['Offer Id'].astype(str)

   
    
    reject_rate_total = pd.merge(reject_total, flow_conv, on=["Time", "Offer Id"], how="left").fillna(0)
    reject_rate_total["reject rate"] = reject_rate_total["Total reject"] / (reject_rate_total["Total reject"] + reject_rate_total["Total Conversions"]).replace(0, np.nan)
    
    # 步骤d：计算每个affiliate的reject rate
    reject_affiliate = reject_event.groupby(["Time", "Offer Id", "Affiliate"]).agg({
        "是否为reject": "sum"
    }).reset_index().rename(columns={"是否为reject": "Total reject"})
    
    flow_conv_aff = flow_df.groupby(["Time", "Offer ID", "Advertiser", "Affiliate", "App ID", "GEO"]).agg({
        "Total Conversions": "sum"
    }).reset_index().rename(columns={"Offer ID": "Offer Id"})

    
    reject_affiliate['Offer Id']=reject_affiliate['Offer Id'].astype(str)
    flow_conv_aff['Offer Id']=flow_conv_aff['Offer Id'].astype(str)
    reject_rate_affiliate = pd.merge(reject_affiliate, flow_conv_aff, on=["Time", "Offer Id", "Affiliate"], how="left").fillna(0)
    reject_rate_affiliate["reject rate"] = reject_rate_affiliate["Total reject"] / (reject_rate_affiliate["Total reject"] + reject_rate_affiliate["Total Conversions"]).replace(0, np.nan)
    
    

    # 匹配总体reject rate
    reject_rate_affiliate = pd.merge(
        reject_rate_affiliate,
        reject_rate_total[["Time", "Offer Id", "reject rate"]].rename(columns={"reject rate": "总体 reject rate"}),
        on=["Time", "Offer Id"],
        how="left"
    )
    
    
    # 2、计算非reject事件（核心修正：将 == False 改为 != True）
    non_reject_event = event_df[event_df["是否为reject"] != True].copy()
    
    # 步骤f：计算总体event rate
    non_reject_total = non_reject_event.groupby(["Time", "Offer Id", "Event"]).agg({
        "是否为reject": "count"
    }).reset_index().rename(columns={"是否为reject": "Total event"})
    

    
    
    
    event_rate_total = pd.merge(non_reject_total, flow_conv, on=["Time", "Offer Id"], how="left").fillna(0)
    event_rate_total["event rate"] = event_rate_total["Total event"] / ( event_rate_total["Total Conversions"]).replace(0, np.nan)
    
    # 步骤d：计算每个affiliate的event rate
    non_reject_affiliate_event = event_df[event_df["是否为reject"] != True].copy()
    non_reject_affiliate = non_reject_affiliate_event.groupby(["Time", "Offer Id", "Affiliate", "Event"]).agg({
        "是否为reject": "count"
    }).reset_index().rename(columns={"是否为reject": "Total event"})

    event_rate_affiliate = pd.merge(non_reject_affiliate, flow_conv_aff, on=["Time", "Offer Id", "Affiliate"], how="left").fillna(0)
    event_rate_affiliate["event rate"] = event_rate_affiliate["Total event"] / (event_rate_affiliate["Total Conversions"]).replace(0, np.nan)
    
    # 匹配总体event rate
    event_rate_affiliate = pd.merge(
        event_rate_affiliate,
        event_rate_total[["Time", "Offer Id", "Event", "event rate"]].rename(columns={"event rate": "总体 event rate"}),
        on=["Time", "Offer Id", "Event"],
        how="left"
    )
    
    cols_to_replace = ['GEO', 'App ID', 'Advertiser']
    
    offer_base_info_cols = ['Offer Id'] + cols_to_replace +['Adv Offer ID']
    
    offer_base_info.rename(columns={'offerid': 'Offer Id'}, inplace=True)
    
    offer_base_info['Offer Id'] = offer_base_info['Offer Id'].astype(str)
     
    reject_rate_affiliate = reject_rate_affiliate.merge(
    offer_base_info[offer_base_info_cols],
    on=['Offer Id'],  # 指定共同匹配字段
    how='left',  # 左连接：保留df_a的所有行
    suffixes=('', '_offer_base_info'))
    
    event_rate_affiliate = event_rate_affiliate.merge(
    offer_base_info[offer_base_info_cols],
    on=['Offer Id'],  # 指定共同匹配字段
    how='left',  # 左连接：保留df_a的所有行
    suffixes=('', '_offer_base_info'))  # 原列名不加后缀，b的列加_b后缀
    
    
    for col in cols_to_replace:

        event_rate_affiliate[col] = event_rate_affiliate[f'{col}_offer_base_info'].fillna(event_rate_affiliate[col])
        reject_rate_affiliate[col] = reject_rate_affiliate[f'{col}_offer_base_info'].fillna(reject_rate_affiliate[col])      

    reject_rate_affiliate = reject_rate_affiliate.drop(columns=[f'{col}_offer_base_info' for col in cols_to_replace])    
    event_rate_affiliate = event_rate_affiliate.drop(columns=[f'{col}_offer_base_info' for col in cols_to_replace])
    
    
  
    
    return reject_rate_affiliate, event_rate_affiliate



def calculate_budget_rules(sheets,offer_base_info):
      
    
    df_30d_flow = sheets['流水数据'].copy()
    
    df_reject_rule = sheets['reject规则'].copy()
    df_adv_mapping = sheets['广告主匹配'].copy()
    df_event = sheets['event事件'].copy()
    df_daily_target =sheets['日均目标流水'].copy()
    df_blacklist = sheets['预算黑名单'].copy()
    df_traffic_type = sheets['流量类型'].copy()

  
    df_30d_flow.columns = df_30d_flow.columns.str.strip()
    df_adv_mapping.columns = df_adv_mapping.columns.str.strip()
    df_traffic_type.columns = df_traffic_type.columns.str.strip()
    df_blacklist.columns = df_blacklist.columns.str.strip()
    
 
   
    # 数据预处理：统一列名格式（去除空格/特殊字符）
    for df in [df_30d_flow, df_adv_mapping, df_blacklist, df_traffic_type]:
        df.columns = df.columns.str.strip().str.replace(" ", "").str.replace("—", "-")
        
        
    target_col = 'TotalCaps'

    # 步骤1：尝试转换为数值类型，无法转换的变为NaN
    df_30d_flow[target_col] = pd.to_numeric(df_30d_flow[target_col], errors='coerce')

    # 步骤2：筛选条件：非数字(NaN) 或 数值≤0
    condition = (df_30d_flow[target_col].isna()) | (df_30d_flow[target_col] <= 0)
    
    df_30d_flow.loc[condition, target_col] = 100
    
    
    # ===================== 2. 核心预处理：确保所有关键字段存在 =====================
    # 2.1 检查并补充df_30d_flow的核心字段
    required_flow_cols = [
        "OfferID", "Advertiser", "AppID", "GEO", "Time", "TotalClicks", 
        "TotalConversions", "TotalRevenue", "TotalCost", "TotalProfit", "Status"
    ]
    flow_col_mapping = {
        "Offer ID": "OfferID",
        "App ID": "AppID",
        "Total Clicks": "TotalClicks",
        "Total Conversions": "TotalConversions",
        "Total Revenue": "TotalRevenue",
        "Total Cost": "TotalCost",
        "Total Profit": "TotalProfit"
    }
    df_30d_flow.rename(columns=flow_col_mapping, inplace=True)
    for col in required_flow_cols:
        if col not in df_30d_flow.columns:
            df_30d_flow[col] = np.nan if col != "Time" else pd.NaT
            print(f"警告：df_30d_flow 缺失字段 {col}，已创建空值列")
    
    # 2.2 检查并补充df_adv_mapping的核心字段
    required_adv_cols = ["Advertiser", "流量匹配逻辑"]
    adv_col_mapping = {"流量匹配规则": "流量匹配逻辑", "匹配逻辑": "流量匹配逻辑"}
    for old_col, new_col in adv_col_mapping.items():
        if old_col in df_adv_mapping.columns:
            df_adv_mapping.rename(columns={old_col: new_col}, inplace=True)
    if "流量匹配逻辑" not in df_adv_mapping.columns:
        df_adv_mapping["流量匹配逻辑"] = ""
        print(f"警告：df_adv_mapping 缺失字段 流量匹配逻辑，已创建空值列")
    
    # 2.3 合并流量匹配逻辑到df_30d_flow
    df_30d_flow = df_30d_flow.merge(
        df_adv_mapping[["Advertiser", "流量匹配逻辑"]].drop_duplicates(),
        on="Advertiser",
        how="left"
    )
    df_30d_flow["流量匹配逻辑"] = df_30d_flow["流量匹配逻辑"].fillna("")
    
    # 2.4 时间字段处理
    df_30d_flow["Time"] = pd.to_datetime(df_30d_flow["Time"], errors="coerce")
    df_30d_flow = df_30d_flow.dropna(subset=["Time"])

    
    # ===================== 3. 提取Offer基础信息 =====================
    df_offer_base = df_30d_flow[
        ["OfferID", "Advertiser", "AppID", "GEO", "AdvOfferID", "Payin", "TotalCaps"]
    ].drop_duplicates(subset=["OfferID"], keep="first")
    for col in ["AdvOfferID", "Payin", "TotalCaps"]:
        if col not in df_offer_base.columns:
            df_offer_base[col] = np.nan
    
    # ===================== 4. 时间范围定义 =====================
    max_date_in_data = df_30d_flow["Time"].max()
    last_30d_start = max_date_in_data - timedelta(days=29)
    last_1d_start = max_date_in_data
    
    # 筛选数据
    df_30d_filtered = df_30d_flow[
        (df_30d_flow["Time"] >= last_30d_start) & 
        (df_30d_flow["Time"] <= max_date_in_data)
    ].copy()
    df_1d_filtered = df_30d_flow[df_30d_flow["Time"] == last_1d_start].copy()
    
    # ===================== 5. 通用指标计算函数（补充1天全指标+1d_STATUS） =====================
    def calculate_agg_metrics(df, group_cols, period_name):
        df_merged = df.merge(
            df_offer_base[["OfferID", "AppID", "GEO"]],
            on="OfferID",
            how="left",
            suffixes=("", "_base")
        )
        df_merged["AppID"] = df_merged["AppID"].fillna(df_merged["AppID_base"]).fillna("未知")
        df_merged["GEO"] = df_merged["GEO"].fillna(df_merged["GEO_base"]).fillna("未知")
        df_merged.drop(columns=["AppID_base", "GEO_base"], errors="ignore", inplace=True)
        
        # 整体汇总（补充完整的1d/30d指标：Clicks/Conversions/Revenue/Cost/Profit + STATUS）
        agg_total = df_merged.groupby([*group_cols, "AppID", "GEO"], dropna=False).agg(
            **{
                f"{period_name}_TotalClicks": ("TotalClicks", lambda x: x.fillna(0).sum()),
                f"{period_name}_TotalConversions": ("TotalConversions", lambda x: x.fillna(0).sum()),
                f"{period_name}_TotalRevenue": ("TotalRevenue", lambda x: x.fillna(0).sum()),
                f"{period_name}_TotalCost": ("TotalCost", lambda x: x.fillna(0).sum()),
                f"{period_name}_TotalProfit": ("TotalProfit", lambda x: x.fillna(0).sum()),
                f"{period_name}_STATUS": ("Status", lambda x: x.dropna().iloc[0] if not x.dropna().empty else "UNKNOWN")
            }
        ).reset_index()
        
        # 计算CR（转化率）
        agg_total[f"{period_name}_CR"] = np.where(
            agg_total[f"{period_name}_TotalClicks"] > 0,
            agg_total[f"{period_name}_TotalConversions"] / agg_total[f"{period_name}_TotalClicks"],
            0
        )
        
        # Affiliate维度汇总（补充完整的Aff指标）
        agg_aff = pd.DataFrame()
        if "Affiliate" in df_merged.columns:
            agg_aff = df_merged.groupby([*group_cols, "AppID", "GEO", "Affiliate"], dropna=False).agg(
                **{
                    f"{period_name}_AffClicks": ("TotalClicks", lambda x: x.fillna(0).sum()),
                    f"{period_name}_AffConversions": ("TotalConversions", lambda x: x.fillna(0).sum()),
                    f"{period_name}_AffRevenue": ("TotalRevenue", lambda x: x.fillna(0).sum()),
                    f"{period_name}_AffCost": ("TotalCost", lambda x: x.fillna(0).sum()),
                    f"{period_name}_AffProfit": ("TotalProfit", lambda x: x.fillna(0).sum())
                }
            ).reset_index()
            
            agg_aff = agg_aff.merge(
                agg_total[[*group_cols, f"{period_name}_TotalRevenue"]],
                on=group_cols,
                how="left"
            )
            agg_aff[f"{period_name}_AffCR"] = np.where(
                agg_aff[f"{period_name}_AffClicks"] > 0,
                agg_aff[f"{period_name}_AffConversions"] / agg_aff[f"{period_name}_AffClicks"],
                0
            )
            agg_aff[f"{period_name}_AffRevenueRatio"] = np.where(
                agg_aff[f"{period_name}_TotalRevenue"] > 0,
                agg_aff[f"{period_name}_AffRevenue"] / agg_aff[f"{period_name}_TotalRevenue"],
                0
            )
            
            aff_summary = agg_aff.groupby(group_cols).apply(
                lambda x: "\n".join([
                    f"Affiliate: {row['Affiliate']} | Clicks: {row[f'{period_name}_AffClicks']:.0f} | Conversions: {row[f'{period_name}_AffConversions']:.0f} | CR: {row[f'{period_name}_AffCR']:.4f} | Cost: {row[f'{period_name}_AffCost']:.2f} | Profit: {row[f'{period_name}_AffProfit']:.2f} | Revenue占比: {row[f'{period_name}_AffRevenueRatio']:.4f}"
                    for _, row in x.iterrows()
                ])
            ).reset_index(name=f"{period_name}_AffiliateSummary")
            
            agg_total = agg_total.merge(aff_summary, on=group_cols, how="left")
        else:
            agg_total[f"{period_name}_AffiliateSummary"] = "无Affiliate数据"
        
        return agg_total, agg_aff
    
    # ===================== 6. 计算30天/1天指标（含完整1d指标+1d_STATUS） =====================
    group_cols = ["OfferID", "Advertiser"]
    df_30d_metrics, df_30d_aff_metrics = calculate_agg_metrics(df_30d_filtered, group_cols, "30d")
    df_1d_metrics, df_1d_aff_metrics = calculate_agg_metrics(df_1d_filtered, group_cols, "1d")
    
    # 计算剩余Cap
    df_1d_metrics = df_1d_metrics.merge(
        df_offer_base[["OfferID", "TotalCaps"]],
        on="OfferID",
        how="left"
    )
    df_1d_metrics["TotalCaps"] = pd.to_numeric(df_1d_metrics["TotalCaps"], errors="coerce").fillna(100)
    df_1d_metrics["RemainingCap"] =  (df_1d_metrics["TotalCaps"]-df_1d_metrics["1d_TotalConversions"]).fillna(df_1d_metrics["TotalCaps"])
    
    # ===================== 7. 筛选合格Offer =====================
    daily_revenue = df_30d_flow.groupby(["OfferID", "Time"])["TotalRevenue"].sum().reset_index()
    qualified_offers = daily_revenue[daily_revenue["TotalRevenue"].fillna(0) >= 10]["OfferID"].unique()
    df_qualified = df_30d_metrics[df_30d_metrics["OfferID"].isin(qualified_offers)].copy()
    
    # ===================== 8. 补充基础信息（关联1d_STATUS替换30d_STATUS） =====================
    flow_logic_df = df_30d_flow[["OfferID", "流量匹配逻辑"]].drop_duplicates(subset=["OfferID"])
    df_qualified = df_qualified.merge(
        df_offer_base[["OfferID", "AdvOfferID", "Payin", "TotalCaps"]],
        on="OfferID",
        how="left"
    )
    df_qualified = df_qualified.merge(
        flow_logic_df,
        on="OfferID",
        how="left"
    )
    df_qualified["流量匹配逻辑"] = df_qualified["流量匹配逻辑"].fillna("")
    df_qualified = df_qualified.merge(
        df_1d_metrics[["OfferID", "Advertiser", "RemainingCap", "1d_STATUS"]],  # 关联1d_STATUS
        on=["OfferID", "Advertiser"],
        how="left"
    )
    df_qualified["TotalCaps"] = pd.to_numeric(df_qualified["TotalCaps"], errors="coerce").fillna(100)
    # 填充RemainingCap空值为TotalCaps
    df_qualified["RemainingCap"] = df_qualified["RemainingCap"].fillna(df_qualified["TotalCaps"])

    # 替换30d_STATUS为1d_STATUS（核心修改）
    df_qualified["30d_STATUS"] = df_qualified["1d_STATUS"].fillna(df_qualified["30d_STATUS"])
    df_qualified.drop(columns=["1d_STATUS"], errors="ignore", inplace=True)
    
    # 补充完整的1天维度指标到df_qualified
    df_qualified = df_qualified.merge(
        df_1d_metrics[["OfferID", "Advertiser", "1d_TotalClicks", "1d_TotalConversions", 
                      "1d_TotalRevenue", "1d_TotalCost", "1d_TotalProfit","1d_AffiliateSummary"]],
        on=["OfferID", "Advertiser"],
        how="left"
    )
    one_day_cols = [
        "1d_TotalClicks", 
        "1d_TotalConversions", 
        "1d_TotalRevenue", 
        "1d_TotalCost", 
        "1d_TotalProfit"
    ]
    # 遍历列名，填充空值为0，并转换为数值类型
    for col in one_day_cols:
        # 先转换为数值类型（处理可能的非数值数据），再填充空值
        df_qualified[col] = pd.to_numeric(df_qualified[col], errors="coerce").fillna(0)
    
    # ===================== 9. 流量匹配与黑名单过滤 =====================
    def match_traffic_affiliate(row):
        traffic_logic = row.get("流量匹配逻辑", "")
        if not traffic_logic:
            return []
        
        keywords = traffic_logic.split("/")
        traffic_type_col = "流量类型--一级分类" if "流量类型--一级分类" in df_traffic_type.columns else "流量类型"
        if traffic_type_col not in df_traffic_type.columns:
            return []
        
        mask = df_traffic_type[traffic_type_col].str.contains("|".join(keywords), na=False)
        df_matched = df_traffic_type[mask].copy()
        
        if "inapp流量" in traffic_logic or "inapp 流量" in traffic_logic:
            priority_col = "非100%xdj新预算推量优先级"
        else:
            priority_col = "纯xdj新预算推量优先级"
        
        if priority_col in df_matched.columns:
            df_matched = df_matched[df_matched[priority_col] != "不沟通"]
        
        return df_matched["Affiliate"].tolist() if not df_matched.empty else []
    
    df_qualified["AvailableAffiliates"] = df_qualified.apply(match_traffic_affiliate, axis=1)
    df_qualified = df_qualified.explode("AvailableAffiliates").rename(columns={"AvailableAffiliates": "Affiliate"})
    df_qualified["Affiliate"] = df_qualified["Affiliate"].fillna("未知")
    
    # 过滤黑名单
    blacklist_all = df_blacklist[(df_blacklist["Affiliate"] == "All")]["OfferID"].unique() if "Affiliate" in df_blacklist.columns else []
    df_qualified = df_qualified[~df_qualified["OfferID"].isin(blacklist_all)]
    
    if "Affiliate" in df_blacklist.columns and "OfferID" in df_blacklist.columns:
        blacklist_specific = df_blacklist[df_blacklist["Affiliate"] != "All"][["OfferID", "Affiliate"]]
        df_qualified = df_qualified.merge(
            blacklist_specific,
            on=["OfferID", "Affiliate"],
            how="left",
            indicator=True
        )
        df_qualified = df_qualified[df_qualified["_merge"] == "left_only"].drop(columns=["_merge"])
    
    # 筛选条件改为使用1d_STATUS（原30d_STATUS已替换）
    df_qualified = df_qualified[df_qualified["30d_STATUS"] == "ACTIVE"]
    
    # ===================== 10. 待办事项标记（规则a/c/d） =====================
    df_qualified["待办事项标记"] = ""
    
    
    # 补充Affiliate收入字段
    if not df_1d_aff_metrics.empty:
        merge_cols = [col for col in [*group_cols, "Affiliate", "1d_AffRevenue"] if col in df_1d_aff_metrics.columns]
        df_qualified = df_qualified.merge(
            df_1d_aff_metrics[merge_cols],
            on=[col for col in merge_cols if col != "1d_AffRevenue"],
            how="left"
        )
    else:
        df_qualified["1d_AffRevenue"] = 0
    
    if not df_30d_aff_metrics.empty:
        merge_cols = [col for col in [*group_cols, "Affiliate", "30d_AffRevenue"] if col in df_30d_aff_metrics.columns]
        df_qualified = df_qualified.merge(
            df_30d_aff_metrics[merge_cols],
            on=[col for col in merge_cols if col != "30d_AffRevenue"],
            how="left"
        )
    else:
        df_qualified["30d_AffRevenue"] = 0
    
    # 规则a：剩余Cap<0 → 沟通加预算
    mask_a = df_qualified["RemainingCap"].fillna(0) < 0
    df_qualified.loc[mask_a, "待办事项标记"] = "和广告主沟通是否可以加预算"

    # 沟通加预算记录的Affiliate置空 + 去重
    df_qualified.loc[df_qualified["待办事项标记"] == "和广告主沟通是否可以加预算", "Affiliate"] = ""
    dedup_cols = ["OfferID", "Advertiser", "AppID", "GEO", "Affiliate", "待办事项标记"]
    df_qualified = df_qualified.drop_duplicates(subset=dedup_cols, keep="first")
    
    # 规则c：昨日有收入 → 推满预算（使用1d_TotalRevenue判断）
    mask_c = (df_qualified["待办事项标记"] == "") & (df_qualified["1d_AffRevenue"].fillna(0) > 0)
    df_qualified.loc[mask_c, "待办事项标记"] = "该流量昨日有产生流水，推流量把预算跑满"
    
    # 规则d：近30天有收入但昨日无 → 持续跑预算
    mask_d = (df_qualified["待办事项标记"] == "") & (df_qualified["30d_AffRevenue"].fillna(0) > 0) & (df_qualified["1d_AffRevenue"].fillna(0) == 0)
    df_qualified.loc[mask_d, "待办事项标记"] = "该流量近30天内有产生流水，但昨日无产生流水，推流量持续跑预算"
    
    # ===================== 11. 核心逻辑i：规则e（匹配昨日有流水的其他Offer） =====================
    def match_other_offer(row):
        # 提取核心字段
        affiliate = row.get("Affiliate")
        app_id = row.get("AppID")
        geo = row.get("GEO")
        current_offer = row.get("OfferID")
        traffic_logic = row.get("流量匹配逻辑", "")
        
        # 动态确定优先级字段
        if "Inapp流量" in traffic_logic or "inapp流量" in traffic_logic:
            priority_col = "非100%xdj新预算推量优先级"
        else:
            priority_col = "纯xdj新预算推量优先级"
        
        # 获取优先级文本
        priority_text = ""
        if (affiliate not in ["未知", ""] and pd.notna(affiliate) and 
            priority_col in df_traffic_type.columns and 
            affiliate in df_traffic_type["Affiliate"].values):
            priority_text = df_traffic_type[df_traffic_type["Affiliate"] == affiliate][priority_col].iloc[0]
        priority_text = priority_text if priority_text else "无明确推量优先级指引"
        
        # 基础校验
        if pd.isna(app_id) or pd.isna(geo) or affiliate in ["未知", ""] or pd.isna(affiliate):
            return f"按照{priority_col}指引进行操作：{priority_text}"
        
        # 构建昨日流水数据集（含完整1d指标）
        df_1d_full = df_1d_metrics[["OfferID", "Advertiser", "AppID", "GEO", 
                                   "1d_TotalRevenue", "1d_TotalClicks", "1d_TotalConversions",
                                   "1d_TotalCost", "1d_TotalProfit"]].copy()
        if not df_1d_aff_metrics.empty:
            df_1d_full = df_1d_full.merge(
                df_1d_aff_metrics[["OfferID", "Advertiser", "Affiliate"]].drop_duplicates(),
                on=["OfferID", "Advertiser"],
                how="left"
            )
        
        # 筛选同Affiliate+AppID+GEO下昨日有流水的其他Offer
        mask_match = (
            (df_1d_full["Affiliate"] == affiliate) &
            (df_1d_full["AppID"] == app_id) &
            (df_1d_full["GEO"] == geo) &
            (df_1d_full["OfferID"] != current_offer) &
            (df_1d_full["1d_TotalRevenue"].fillna(0) > 0)
        )
        df_matched_offers = df_1d_full[mask_match].copy()
        
        # 按昨日流水降序取最高
        top_offer = None
        if not df_matched_offers.empty:
            df_sorted = df_matched_offers.sort_values("1d_TotalRevenue", ascending=False)
            top_offer = df_sorted.iloc[0]
        
        # 返回文案
        if top_offer is None:
            return f"按照{priority_col}指引进行操作：{priority_text}"
        else:
            return (
                f"该流量已经在其他offerid相同预算下(状态为暂停或者预算不足)产生流水，具体预算信息为Offer ID：{top_offer['OfferID']}、App ID：{top_offer['AppID']}、GEO：{top_offer['GEO']}、Advertiser：{top_offer['Advertiser']}，"
                f"对应昨日流水是{top_offer['1d_TotalRevenue']:.2f}美金（昨日点击：{top_offer['1d_TotalClicks']:.0f}，转化：{top_offer['1d_TotalConversions']:.0f}，成本：{top_offer['1d_TotalCost']:.2f}，利润：{top_offer['1d_TotalProfit']:.2f}），"
                f"和流量沟通push新预算，新增预算预算按照{priority_col}指引进行操作：{priority_text}"
            )
    
    # 规则e筛选
    mask_e = df_qualified["待办事项标记"] == ""
    mask_budgeted = ~mask_e
    df_budgeted = df_qualified[mask_budgeted][["Affiliate", "GEO", "AppID"]].drop_duplicates()
    df_qualified["match_key"] = df_qualified["Affiliate"].fillna("") + "|" + df_qualified["GEO"].fillna("") + "|" + df_qualified["AppID"].fillna("")
    df_budgeted["match_key"] = df_budgeted["Affiliate"].fillna("") + "|" + df_budgeted["GEO"].fillna("") + "|" + df_budgeted["AppID"].fillna("")
    mask_e_filtered = mask_e & (~df_qualified["match_key"].isin(df_budgeted["match_key"].tolist()))
    df_e = df_qualified[mask_e_filtered].copy()
    df_qualified.drop(columns=["match_key"], inplace=True)
    
    # 应用核心逻辑i
    df_e["待办事项标记"] = df_e.apply(match_other_offer, axis=1)
    df_qualified.loc[mask_e_filtered, "待办事项标记"] = df_e["待办事项标记"]
    
    mask_keep = mask_budgeted | mask_e_filtered
    df_qualified = df_qualified[mask_keep].copy()

    # 2. 删除临时匹配键列
    df_qualified.drop(columns=["match_key"], inplace=True, errors="ignore")
    
    # ===================== 12. 核心逻辑ii：仅针对规则e——按Affiliate+AppID+GEO保留组内最高流水Offer =====================
    # 步骤1：拆分规则a/c/d和规则e（规则a/c/d完整保留）
    mask_acd = df_qualified["待办事项标记"].isin([
        "和广告主沟通是否可以加预算",
        "该流量昨日有产生流水，推流量把预算跑满",
        "该流量近30天内有产生流水，但昨日无产生流水，推流量持续跑预算"
    ])
    df_acd = df_qualified[mask_acd].copy()
    
    # 提取规则e数据（去除a/c/d后的所有行）
    df_e = df_qualified[~mask_acd].copy()
    
    # 步骤2：仅对规则e执行核心逻辑（你的需求）
    if not df_e.empty:
        # 1. 处理空值，避免分组错误（不影响核心逻辑）
        df_e["Affiliate"] = df_e["Affiliate"].fillna("未知")
        df_e["AppID"] = df_e["AppID"].fillna("未知")
        df_e["GEO"] = df_e["GEO"].fillna("未知")
        df_e["30d_TotalRevenue"] = pd.to_numeric(df_e["30d_TotalRevenue"], errors="coerce").fillna(0)
        
        # 2. 关键：按Affiliate+AppID+GEO分组，对每个组内的OfferID按流水降序排序
        #    排序后，每组第一行就是流水最高的OfferID
        df_e_sorted = df_e.sort_values(
            by=["Affiliate", "AppID", "GEO", "30d_TotalRevenue"],
            ascending=[True, True, True, False]  # 流水降序，保证最高的在最前
        )
        
        # 3. 去重：每个Affiliate+AppID+GEO只保留第一行（流水最高的OfferID）
        df_e_final = df_e_sorted.drop_duplicates(
            subset=["Affiliate", "AppID", "GEO"],
            keep="first"
        ).reset_index(drop=True)
    else:
        df_e_final = pd.DataFrame()
    
    # 步骤3：合并最终数据（规则a/c/d + 规则e去重后）
    df_final = pd.concat([df_acd, df_e_final], ignore_index=True)
                         
    
    # ===================== 13. 待办事项排序 =====================
    last_1d_weekday = max_date_in_data.weekday()
    df_final["待办事项排序"] = ""
    
    #步骤1：提取Advertiser+OfferID的唯一组合，保留30d_TotalRevenue
    df_rank_base = df_final[["Advertiser", "OfferID", "30d_TotalRevenue"]].drop_duplicates(
    subset=["Advertiser", "OfferID"],  # 确保每个OfferID在每个Advertiser下只算一次
    keep="first"  # 保留第一条记录（同一OfferID的30d_TotalRevenue值一致）
      )

    #步骤2：按Advertiser分组，对30d_TotalRevenue降序计算唯一排序
    df_rank_base["排序"] = df_rank_base.groupby("Advertiser")["30d_TotalRevenue"].rank(
    ascending=False,
    method="first"  # 相同营收时按出现顺序排名，避免并列
    ).astype(int)

    #步骤3：将唯一排序值关联回原数据（同一个OfferID会获得相同排序）
    df_final = df_final.merge(
    df_rank_base[["Advertiser", "OfferID", "排序"]],
    on=["Advertiser", "OfferID"],
    how="left"
     ) 
    
     
    def is_similar_name(row):
      # 去除两端空白，统一转为小写（避免大小写干扰）
      adv = str(row["Advertiser"]).strip().lower()
      aff = str(row["Affiliate"]).strip().lower()
    
      # 排除空值情况
      if not adv or not aff:
        return False
       # 判断核心包含关系：一个字符串是另一个的子串（且不是完全空白）
      if (adv in aff) or (aff in adv):
        return True
      special_pair = {"leapmob", "metabits"}
      
      if {adv, aff} == special_pair:
          return True
          
      return False
  
    mask_similar = df_final.apply(
    is_similar_name, axis=1)
    
    df_final = df_final[~mask_similar].reset_index(drop=True)
    
    
    target_date = datetime.now().date()
    
    start_of_week = target_date - timedelta(days=target_date.weekday())
    
    days_in_week_so_far = [
        start_of_week + timedelta(days=i) 
        for i in range((target_date - start_of_week).days + 1)
    ]
    workdays = [d for d in days_in_week_so_far if is_workday(d)]
    workdays_count = len(workdays)
    
    
    
    
    # 规则1：昨日有流水 + 第一个工作日+排序前10 → 第一优先级
    mask_p1 = (df_final["待办事项标记"] == "该流量昨日有产生流水，推流量把预算跑满")&(df_final["排序"] <= 10)
    
    mask_p2 = (df_final["待办事项标记"] == "和广告主沟通是否可以加预算") 
    
    mask_p3 = (df_final["待办事项标记"] == "该流量近30天内有产生流水，但昨日无产生流水，推流量持续跑预算") & (df_final["排序"] <= 10) 
    
    mask_p4 = (df_final["待办事项标记"].str.contains(r"流量已经在其他offerid相同预算下.*状态为暂停或者预算不足.*产生流水")) & (df_final["排序"] <= 10) 

    mask_all_rules = mask_p1 | mask_p2 | mask_p3|mask_p4
    
    mask_p5 = (df_final["排序"] <= 3) & (~mask_all_rules)
    
    
    if workdays_count==1:
        df_final.loc[mask_p1, "待办事项排序"] = "今日第一优先级待办"
    elif workdays_count==2:
        df_final.loc[mask_p2, "待办事项排序"] = "今日第一优先级待办"
        df_final.loc[mask_p3, "待办事项排序"] = "今日第二优先级待办"
    elif workdays_count==3:
        df_final.loc[mask_p4, "待办事项排序"] = "今日第一优先级待办"
        df_final.loc[mask_p5, "待办事项排序"] = "今日第二优先级待办"

    

    
  
    # 定义输出列（包含完整的1天维度指标）
    output_cols = [
        "OfferID", "Advertiser", "AdvOfferID", "AppID", "GEO", "Affiliate", "Payin", "TotalCaps",
        # 30天指标
        "30d_TotalClicks", "30d_TotalConversions", "30d_CR", "30d_TotalRevenue", 
        "30d_TotalCost", "30d_TotalProfit", "30d_STATUS", "30d_AffiliateSummary",
        # 1天指标（完整）
        "1d_TotalClicks", "1d_TotalConversions", "1d_TotalRevenue", 
        "1d_TotalCost", "1d_TotalProfit", "1d_AffiliateSummary",'1d_AffRevenue', '30d_AffRevenue'
        # 其他字段
        "RemainingCap", "排序", "待办事项标记", "待办事项排序"
    ]
    output_cols = [col for col in output_cols if col in df_final.columns]
    
    # 最终结果去重
    final_output = df_final[output_cols].drop_duplicates().reset_index(drop=True)
    
    # 还原字段名为带空格的格式
    reverse_col_mapping = {
        "OfferID": "Offer ID",
        "AppID": "App ID",
        "AdvOfferID": "Adv Offer ID",
        "TotalCaps": "Total Caps",
        "RemainingCap": "Remaining_Cap",
        "30d_TotalClicks": "30d_Total Clicks",
        "30d_TotalConversions": "30d_Total Conversions",
        "30d_TotalRevenue": "30d_Total Revenue",
        "30d_TotalCost": "30d_Total Cost",
        "30d_TotalProfit": "30d_Total Profit",
        "30d_AffiliateSummary": "30d_Affiliate_Summary",
        "1d_TotalClicks": "1d_Total Clicks",
        "1d_TotalConversions": "1d_Total Conversions",
        "1d_TotalRevenue": "1d_Total Revenue",
        "1d_TotalCost": "1d_Total Cost",
        "1d_TotalProfit": "1d_Total Profit",
        "1d_AffiliateSummary":'1d_Affiliate_Summary'
    }
    final_output.rename(columns=reverse_col_mapping, inplace=True)
    
    return final_output


# -------------------------- Streamlit 页面逻辑 --------------------------
def download_github_template():
    """从GitHub下载模板文件"""
    try:
        response = requests.get(GITHUB_TEMPLATE_URL, timeout=10)
        response.raise_for_status()
        return BytesIO(response.content)
    except Exception as e:
        st.error(f"模板下载失败：{str(e)}")
        return None



def main():
    st.title("📊 广告数据分析工具")
    st.divider()
    
    # 侧边栏 - 模板下载
    with st.sidebar:
        st.subheader("📋 模板下载")
        template_file = download_github_template()
        if template_file:
            st.download_button(
                label="下载Excel模板文件",
                data=template_file,
                file_name="adv_report_template.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        
        st.divider()
        st.info("""
        ### 使用说明
        1. 下载模板文件并按格式填写数据
        2. 上传填写好的Excel文件
        3. 点击「开始分析」按钮
        4. 查看分析结果并下载最终报告
        """)
    
    # 主页面 - 文件上传
    st.subheader("📤 上传数据文件")
    uploaded_file = st.file_uploader(
        "请上传填写好的Excel文件",
        type=["xlsx"],
        help="请确保文件包含所有必要的sheet：流水数据、reject规则、广告主匹配等"
    )    
    if uploaded_file is not None:
        try:
            # 加载数据
            with st.spinner("正在加载数据..."):
                sheets, offer_base_info = load_excel_template(uploaded_file)
            st.success("数据加载成功！")
            
            # 开始分析
            if st.button("🚀 开始分析", type="primary"):
                with st.spinner("正在执行数据分析..."):

                      total_data, date_new, date_old = calculate_total_data(sheets)

        

                      
                      budget_fluctuation = calculate_budget_fluctuation(sheets,offer_base_info)

                      
                      reject_event_df = calculate_reject_data(sheets)

                      
                      advertiser_data = calculate_advertiser_data(sheets, date_new, date_old, reject_event_df)

                      
                      affiliate_data = calculate_affiliate_data(sheets, date_new, date_old, reject_event_df)

                      
                      large_drop_budget = calculate_large_drop_budget(sheets,offer_base_info)


                      profit_influence = calculate_profit_influence(sheets, date_new, date_old)

                      
                      final_output = calculate_budget_rules(sheets,offer_base_info)

                      reject_analysis, non_reject_analysis = calculate_event_analysis(sheets,offer_base_info)

    
                     # 3. 合并所有结果到一个Excel（多个sheet）
                      output = BytesIO()
                      with pd.ExcelWriter(output, engine='openpyxl') as writer:
                         total_data.to_excel(writer, sheet_name="1-总数据", index=False)
                         budget_fluctuation.to_excel(writer, sheet_name="2-预算波动", index=False)
                         advertiser_data.to_excel(writer, sheet_name="3-Advertiser数据", index=False)
                         affiliate_data.to_excel(writer, sheet_name="4-Affiliate数据", index=False)
                         large_drop_budget.to_excel(writer, sheet_name="5-流水大幅下降预算", index=False)
                         pd.DataFrame({"利润影响因素分析": [profit_influence]}).to_excel(writer, sheet_name="6-利润影响分析", index=False)
                         reject_analysis.to_excel(writer, sheet_name="7-reject事件分析", index=False)
                         non_reject_analysis.to_excel(writer, sheet_name="8-非reject事件分析", index=False)
                         final_output.to_excel(writer, sheet_name="9-今日待办事项", index=False)
                    # 下载最终报告
                      st.divider()
                      st.download_button(
                        label="📥 下载完整分析报告",
                        data=output,
                        file_name=f"广告数据分析报告_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary")
        
        except Exception as e:
            st.error(f"分析过程出错：{str(e)}")
            st.exception(e)
    else:
        st.info("请上传Excel数据文件开始分析（可先下载模板参考格式）")
    


if __name__ == "__main__":
    main()
